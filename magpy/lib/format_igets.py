import sys
import calendar
from magpy.stream import DataStream, read, magpyversion, join_streams, subtract_streams, merge_streams
from magpy.core.methods import testtime, convert_geo_coordinate, extract_date_from_string
import os
from datetime import datetime, timedelta, timezone
import time
import numpy as np
import logging

#global variables
logger = logging.getLogger(__name__)
KEYLIST = DataStream().KEYLIST
MISSING_DATA = 99999
NOT_REPORTED = 88888

def isGGP(filename):
    """
    Checks whether a file is GGP format.
    """
    try:
        with open(filename, 'rt') as myfi:
            temp = myfi.readline()
    except:
        return False
    try:
        if not temp.startswith('Filename '):
            return False
        if not '.ggp' in temp:
            return False
    except:
        return False
    return True

def read_tides(filename, **kwargs):
    """
    Read tides (ETERNA) data format.
    """
    debug = kwargs.get('debug')
    t0 = kwargs.get('starttime','1900-01-01')
    t1 = kwargs.get('endtime','2100-12-31')

    array = [[] for key in KEYLIST]
    stream = DataStream()
    stream.header = {}

    stream.header["col-x"]='pred.tide'
    stream.header["col-y"]='pole tide'
    stream.header["col-z"]='lod tide'
    stream.header["col-f"]='pred.sig.'
    stream.header["unit-col-x"]='nm/s**2'
    stream.header["unit-col-y"]='nm/s**2'
    stream.header["unit-col-z"]='nm/s**2'
    stream.header["unit-col-f"]='nm/s**2'
    if debug:
        print( stream.header )

    with open(filename, "r") as f:
        getdat=0
        for jj, line in enumerate(f, start=1):
            line = line.strip()
            if len(line)>0 and line[:8]=='77777777':
                getdat=1
                if debug:
                    print( stream.header )
                continue
            if len(line)>0 and line[:8]=='99999999':
                break
            if getdat==1:
                line=line.split()
                timestring=line[0]+'T'+read_eterna_time(line[1])
                try:
                    tt=datetime.strptime(timestring,'%Y%m%dT%H%M%S')
                except ValueError: #sometimes seconds in prd file do not fit, e.g. 72
                    dt=(array[0][1]-array[0][0]) #estimate samplerate
                    tt=array[0][-1]+timedelta(seconds=dt.seconds)
                #second check: soemetimes there are two consecutive 100 (mss) values
                if len(array[0])>0:
                    if tt==array[0][-1]:
                        dt=(array[0][1]-array[0][0]) #estimate samplerate
                        #tt=tt+timedelta(seconds=dt.seconds) #other way round!
                        array[0][-1]=tt-timedelta(seconds=dt.seconds)
                        #print(array[0][-1])
                if tt>=datetime.strptime(t0,'%Y-%m-%d') and tt<=datetime.strptime(t1,'%Y-%m-%d'):
                    array[0].append( tt )
                    array[1].append(float(line[3]))
                    array[2].append(float(line[4]))
                    array[3].append(float(line[5]))
                    array[4].append(float(line[2]))
                    if debug:
                        print(array[0][-1],array[1][-1],array[2][-1])

    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])
    stream = DataStream(header=stream.header,ndarray=np.asarray(array,dtype=object))
    return stream

def readGGP(filename, headonly=False, **kwargs):
    """
    Read GGP data format (IGETS/ETERNA).
    """
    debug = kwargs.get('debug',False)
    t0 = kwargs.get('starttime','1900-01-01')
    t1 = kwargs.get('endtime','2100-12-31')
    if debug:
        print('Read GPP data in lib')
    array = [[] for key in KEYLIST]
    stream = DataStream()
    stream.header = {}

    with open(filename, "r") as f:
        getdat=0
        for jj, line in enumerate(f, start=1):
            line = line.strip()
            if len(line)>0 and line[:8]=='77777777':
                if headonly:
                # skip data for option headonly
                    break
                else:
                    getdat=1
                if debug:
                    print( stream.header )
                continue
            if len(line)>0 and line[:8]=='99999999':
                break
            if len(line)>0 and line[:8]=='yyyymmdd':
                line=line.split()
                stream.header["col-x"]=line[2][0]
                stream.header["col-y"]=line[3][0]
                stream.header["unit-col-x"]=line[2][2]
                stream.header["unit-col-y"]=line[3][2]
            if jj>1 and jj<8:
                k,v=line.split(':')
                stream.header[k.strip()]=v.strip()
            if getdat==1:
                line=line.split()
                timestring=line[0]+'T'+read_eterna_time(line[1])
                tt=datetime.strptime(timestring,'%Y%m%dT%H%M%S')
                if tt>=datetime.strptime(t0,'%Y-%m-%d') and tt<=datetime.strptime(t1,'%Y-%m-%d'):
                    array[0].append( tt )
                    array[1].append(float(line[2]))
                    array[2].append(float(line[3]))
                    # if debug:
                    #     print(array[0][-1],array[1][-1],array[2][-1])

    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])
    stream = DataStream(header=stream.header,ndarray=np.asarray(array,dtype=object))
    return stream

def read_eterna_time(tt):
    """
    Read ETERNA time string an return classical string.
    """
    h,m,s='00','00','00'
    if len(tt)==0:
        return false
    if len(tt)<=2:
        s=tt
    if len(tt)<=4 and len(tt)>2:
        s=tt[-2:]
        m=tt[:-2]
    else:
        s=tt[-2:]
        m=tt[-4:-2]
        h=tt[:-4]
    return h.zfill(2)+m.zfill(2)+s.zfill(2)



def remove_leading_zeros(s: str) -> str:
    stripped = s.lstrip("0")
    result = stripped if stripped else "0"
    return result.rjust(6)


def write_enterna_date(dd):
    """
    Write ETERNA date.
    """
    if isinstance(dd, datetime):
        return dd.strftime('%Y%m%d') + ' ' + remove_leading_zeros( dd.strftime('%H%M%S') )

def replace_nan(s, v, keys=[]):
    """
    Replace numpy nans with desired value.
    """
    for key in keys:
        arr_float = s[key].astype(float)
        arr_clean = np.nan_to_num(arr_float, nan=v)
        #s[key]=np.array(arr_clean, dtype=object)
        s[key]=arr_clean
    return s

def writeGGP(datastream, filename, **kwargs):

    """
    Writing GGP data format (IGETS/ETERNA).
    """
    debug = kwargs.get('debug')
    mode = kwargs.get('mode')
    fillval = kwargs.get('fillvalue')
    if fillval==None:
        fillval=999.999999

    if debug:
        print( datastream.header )
        #print( datastream.length()[0] )
        #print( sys.version_info )

    def OpenFile(filename, mode='w'):
        if sys.version_info >= (3,0,0):
            f = open(filename, mode, newline='')
        else:
            f = open(filename, mode+'b')
        return f


    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(exst,datastream,extend=True)
            myFile= OpenFile(filename)
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(datastream,exst,extend=True)
            myFile= OpenFile(filename)
        elif mode == 'append':
            myFile= OpenFile(filename,mode='a')
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= OpenFile(filename)
    else:
        myFile= OpenFile(filename)

    # header
    myFile.write('Filename'.ljust(20)+': '+os.path.basename(filename).ljust(30)+'\n')
    myFile.write('Station'.ljust(20) + ': ' + ( datastream.header.get( 'StationName','')+', ' + datastream.header.get( 'StationCountry','')).ljust(30) + '\n')
    myFile.write('Instrument'.ljust(20)+': '+datastream.header.get( 'SensorID','').ljust(30)+'\n')
    myFile.write('N. Latitude (deg)'.ljust(20) + ': ' + datastream.header.get('StationLatitude', '').ljust(30) + '\n')
    myFile.write('E. Longitude (deg)'.ljust(20)+': '+datastream.header.get( 'StationLongitude','').ljust(30)+'\n')
    myFile.write('Elevation MSL (m)'.ljust(20) + ': ' + datastream.header.get('StationElevation', '').ljust(30) + '\n')
    myFile.write('Author'.ljust(20) + ': ' + datastream.header.get('StationEmail', '').ljust(30) + '\n')
    myFile.write('\n')
    # myFile.write('yyyymmdd hhmmss gravity(V) pressure(V)')
    myFile.write('yyyymmdd hhmmss gravity(V) pressure(mBar)\n')
    myFile.write('C***********************************\n')

    #check for data gaps and add missing
    dts=filename.split('-')[2]
    year, month = int(filename.split('-')[4][:4]), int(filename.split('-')[4][4:6])
    last_day = calendar.monthrange(year, month)[1]
    #print( dts, year, month, last_day )
    if dts=='MIN':
        t0=datetime(year, month, 1, 0, 0)
        t1= datetime(year, month, last_day, 23, 59)
        delta = timedelta(minutes=1)
    elif dts=='SEC':
        t0=datetime(year, month, 1, 0, 0, 0)
        t1= datetime(year, month, last_day, 23, 59, 59)
        delta = timedelta(seconds=1)
    times = []
    current = t0
    while current <= t1:
        times.append(current)
        current += delta
    #gaps checking and filling
    if len(times)!=datastream.length()[0]:
        if debug:
            print('Data gaps present - add nans')
        harray = [[] for key in KEYLIST]
        harray[0]=np.array(times, dtype=object)
        hstream = DataStream(ndarray=np.asarray(harray, dtype=object))
        final_stream=merge_streams(hstream, datastream, mode='replace')
    else:
        final_stream=datastream.copy()
    #replace nan with igets fillvalue
    final_stream=replace_nan(final_stream,fillval,keys=['x','y'])
    # if debug:
    #     print('Nans converted')


    #data
    # if debug:
    #     print('Writing file')
    for jj in range(final_stream.length()[0]):
        gout = f"{final_stream['x'][jj]:10.6f}"
        pout = f"{final_stream['y'][jj]:10.6f}"
        myFile.write('%s %s %s\n' % (write_enterna_date(final_stream['time'][jj]), str(gout).rjust(10), str(pout).rjust(10)) )
    myFile.write('88888888')
    myFile.close()


if __name__ == '__main__':

    print("TESTING: IGETS/ETERNA FORMAT LIBRARY")


