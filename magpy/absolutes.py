# -*- coding: utf-8 -*-

import sys

import numpy as np
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2

from magpy.stream import loggerabs, magpyversion, basestring, DataStream, example5, example6a
from magpy.core.methods import *
from magpy.core import flagging
from urllib.request import urlopen
import shutil
import string



"""
DESCRIPTION
    The MagPy absolutes package is used to anaylse DI measurements as typically done in geomagnetic observatories
    
CONTENTS
    class AbsoluteDIStrcut():  # original MagPy0.4
         used to store data values of a single DI measurement
         
    class DILineStruct():      # implemented in MagPy1.x
         can handle multiple data from DI measurements
         
         Methods:
         - get_data_list(self)  :  convert the DILineStruct to a Datalist used for displaying and saving
         - get_abs_distruct(self)  :  convert the DILineStruct to the AbsDataStruct used for calculations
         
    class AbsoluteData():      # implemented in MagPy1.x
         Analyzing methods
         
         Methods:
         - add(self) :
         - extend(self,datlst,header) :
         - sorting(self) :
         - _corrangle(self,angle) :
         - _get_max(self, key) :
         - _get_min(self, key) :
         - _get_column(self, key) :
         - _check_coverage(self,datastream, keys=['x','y','z']) :
         - _insert_function_values(self, function, funckeys=['x','y','z','f'], validkeys=None, offset=0.0, debug=False)  :
                         Add a function-output to the selected values of the data stream -> e.g. get baseline
         - _calcdec(self, **kwargs)  : Calculates declination values from input.
         - _calcinc(self, **kwargs)  : Calculates inclination values from input.
         - _h(self, f, inc) :  calculate H from F and inc
         - _z(self, f, inc) :  calculate Z from F and inc
         - calcabsolutes(self, **kwargs)  :  Calculates ansolutes using calcdec and calcinc

    Main methods:
    - _logfile_len(fname, logfilter):
    - deg2degminsec(value): Function to convert deg.decimals to a string with deg min and seconds.decimals
    - abs_read(path_or_url=None, dataformat=None, headonly=False, **kwargs)
    - _abs_read(path_or_url=None, dataformat=None, headonly=False, **kwargs)
    - absolute_analysis()
    
    Removed methods in 2.0.0:
    - removeAbsFilesFTP(**kwargs):
    - getAbsFilesFTP(**kwargs):

    
    METHODS OVERVIEW:
    ----------------------------


|    class          | method  | since version  |  until version  |  runtime test  |  result verification  |  manual  | *tested by     |     
|-------------------| ------  | -------------  |  -------------  |  ------------  |  --------------  |  ------  |-----------------|
| **core.absolutes** |    |                 |               |             |               |         |    |
| AbsoluteDIStrcut  |              |  2.0.0     |           |  yes        |  yes          |  7.1    |     |
| DILineStruct      |  get_data_list |  2.0.0   |           |  yes        |  yes          |  7.1    |    |
| DILineStruct      |  get_abs_distruct |  2.0.0  |         |  yes        |  yes          |  7.1    |     |
| DILineStruct      |  save_di     |  2.0.0     |           |  yes*       |  yes*         |  7.3    | absolute_analysis  | 
| AbsoluteAnalysis  |  add         |  2.0.0     |           |             |               |       | unused?             |
| AbsoluteAnalysis  |  extend      |  2.0.0     |           |             |               |       | unused?             |
| AbsoluteAnalysis  |  sorting     |  2.0.0     |           |             |               |       | unused?             |
| AbsoluteAnalysis  |  _corrangle  |  2.0.0     |           |  yes        |  yes          |  -      | ad._calcdec         |
| AbsoluteAnalysis  |  _get_max    |  2.0.0     |           |  yes        |  yes          |  -      | unused?             |
| AbsoluteAnalysis  |  _get_min    |  2.0.0     |           |  yes        |  yes          |  -      | unused?             |
| AbsoluteAnalysis  |  _get_column |  2.0.0     |           |  yes        |  yes          |  -      | ad._calcdec         |
| AbsoluteAnalysis  |  _check_coverage |  2.0.0 |           |  yes        |               |  7.1    |     |
| AbsoluteAnalysis  |  _insert_function_values |  2.0.0 |    |  yes       |               |  7.1    |     |
| AbsoluteAnalysis  |  _calcdec    |  2.0.0     |           |  yes        |  yes          |  7.1    | ad.calcabsolutes     |
| AbsoluteAnalysis  |  _calcinc    |  2.0.0     |           |  yes        |  yes          |  7.1    | ad.calcabsolutes     |
| AbsoluteAnalysis  |  _h          |  2.0.0     |           |  yes        |  yes          |  -      | ad._calcinc         |
| AbsoluteAnalysis  |  _z          |  2.0.0     |           |  yes        |  yes          |  -      | ad._calcinc         |
| AbsoluteAnalysis  |  calcabsolutes |  2.0.0   |           |             |               |  7.1    |    |
|             | _analyse_di_source |  2.0.0     |           |  yes        |               |  -      |     |
|             | _logfile_len       |  2.0.0     |           |             |               |  -      | unused?     |        
|             | data_for_di        |  2.0.0     |           |  yes*       | yes*          |         | absolutes |
|             | deg2degminsec      |  2.0.0     |           |  yes        |  yes          |  7.2    |     |
| d           | absRead            |  2.0.0     |  2.1.0    |             |               |  -      |     |
|             | abs_read           |  2.0.0     |           |  yes        |               |  7.1    |     |
|             | _abs_read          |  2.0.0     |           |  yes        |               |  -      |     |
| d           | absoluteAnalysis   |  2.0.0     |  2.1.0    |             |               |  -      |     |
|             | absolute_analysis  |  2.0.0     |           |  yes        |  yes          |  7.2    |     |
         
"""

class AbsoluteDIStruct(object):
    def __init__(self, time=0, hc=np.nan, vc=np.nan, res=np.nan, f=np.nan, mu=np.nan, md=np.nan, expectedmire=np.nan,varx=np.nan, vary=np.nan, varz=np.nan, varf=np.nan, var1=np.nan, var2=np.nan, temp=np.nan, person='', di_inst='', f_inst=''):
        self.time = time
        self.hc = hc
        self.vc = vc
        self.res = res
        self.f = f
        self.mu = mu
        self.md = md
        self.expectedmire = expectedmire
        self.varx = varx
        self.vary = vary
        self.varz = varz
        self.varf = varf
        self.var1 = var1
        self.var2 = var2
        self.temp = temp
        self.person = person
        self.di_inst = di_inst
        self.f_inst = f_inst

    def __repr__(self):
        return repr((self.time, self.hc, self.vc, self.res, self.f, self.mu, self.md, self.expectedmire, self.varx, self.vary, self.varz, self.varf, self.var1, self.var2))


class DILineStruct(object):
    def __init__(self, ndi, nf=0, time=np.nan, laser=np.nan, hc=np.nan, vc=np.nan, res=np.nan, ftime=np.nan, f=np.nan, opt=np.nan, t=np.nan, scaleflux=np.nan, scaleangle=np.nan, azimuth=np.nan,
                 pier='', person='', di_inst='', f_inst='', fluxgatesensor ='', inputdate='',
                 stationid=None):
        self.time = ndi*[time]
        self.hc = ndi*[hc]
        self.vc = ndi*[vc]
        self.res = ndi*[res]
        self.opt = ndi*[opt]
        self.laser = ndi*[laser]
        self.ftime = nf*[ftime]
        self.f = nf*[f]
        self.t = t
        self.scaleflux = scaleflux
        self.scaleangle = scaleangle
        self.azimuth = azimuth
        self.person = person
        self.pier = pier
        self.stationid = stationid
        self.di_inst = di_inst
        self.f_inst = f_inst
        self.fluxgatesensor = fluxgatesensor
        self.inputdate = inputdate

    def __repr__(self):
        return repr((self.time, self.hc, self.vc, self.res, self.laser, self.opt, self.ftime, self.f, self.scaleflux, self.scaleangle, self.t, self.azimuth, self.pier, self.person, self.di_inst, self.f_inst, self.fluxgatesensor, self.inputdate))

    def __getitem__(self,key):
        return getattr(self,key)

    @deprecated("renamed")
    def getDataList(self):
        return self.get_data_list()

    def get_data_list(self):
        """
        DEFINITION:
            convert the DILineStruct to a Datalist used for displaying and saving

        RESULT:
            #['# MagPy Absolutes\n', '# Abs-Observer: Leichter\n', '# Abs-Theodolite: T10B_0619H154167_07-2011\n', '# Abs-TheoUnit: deg\n', '# Abs-FGSensor: MAG01H_SerialSensor_SerialElectronic_07-2011\n', '# Abs-AzimuthMark: 180.1044444\n', '# Abs-Pillar: A4\n', '# Abs-Scalar: /\n', '# Abs-Temperature: 6.7C\n', '# Abs-InputDate: 2016-01-26\n', 'Miren:\n', '0.099166666666667  0.098055555555556  180.09916666667  180.09916666667  0.098055555555556  0.096666666666667  180.09805555556  180.09805555556\n', 'Positions:\n', '2016-01-21_13:22:00  93.870555555556  90  1.1\n', '2016-01-21_13:22:30  93.870555555556  90  1.8\n', '2016-01-21_13:27:00  273.85666666667  90  0.1\n', '2016-01-21_13:27:30  273.85666666667  90  0.2\n', '2016-01-21_13:25:30  273.85666666667  270  0.3\n', '2016-01-21_13:26:00  273.85666666667  270  -0.6\n', '2016-01-21_13:24:00  93.845555555556  270  -0.2\n', '2016-01-21_13:24:30  93.845555555556  270  0.4\n', '2016-01-21_13:39:30  0  64.340555555556  -0.3\n', '2016-01-21_13:40:00  0  64.340555555556  0.1\n', '2016-01-21_13:38:00  0  244.34055555556  0\n', '2016-01-21_13:38:30  0  244.34055555556  -0.4\n', '2016-01-21_13:36:00  180  295.67055555556  1.1\n', '2016-01-21_13:36:30  180  295.67055555556  1.2\n', '2016-01-21_13:34:30  180  115.66916666667  0.3\n', '2016-01-21_13:35:00  180  115.66916666667  0.9\n', '2016-01-21_13:34:30  180  115.66916666667  0\n', 'PPM:\n', 'Result:\n']

        EXAMPLE:
            abslinestruct.get_abs_distruct()
        """
        try:
            mu1 = self.hc[0]-((self.hc[0]-self.hc[1])/(self.laser[0]-self.laser[1]))*self.laser[0]
            if np.isnan(mu1):
                mu1 = self.hc[0] # in case of laser(vc0-vc1) = 0
        except:
            mu1 = self.hc[0] # in case of laser(vc0-vc1) = 0
        try:
            md1 = self.hc[2]-((self.hc[2]-self.hc[3])/(self.laser[2]-self.laser[3]))*self.laser[2]
            if np.isnan(md1):
                md1 = self.hc[2] # in case of laser(vc0-vc1) = 0
        except:
            md1 = self.hc[2]
        try:
            mu2 = self.hc[12]-((self.hc[12]-self.hc[13])/(self.laser[12]-self.laser[13]))*self.laser[12]
            if np.isnan(mu2):
                mu2 = self.hc[12] # in case of laser(vc0-vc1) = 0
        except:
            mu2 = self.hc[12]
        try:
            md2 = self.hc[14]-((self.hc[14]-self.hc[15])/(self.laser[14]-self.laser[15]))*self.laser[14]
            if np.isnan(md2):
                md2 = self.hc[14] # in case of laser(vc0-vc1) = 0
        except:
            md2 = self.hc[14]

        datalist = []
        # Construct header
        datalist.append('# MagPy Absolutes\n')
        datalist.append('# Abs-Observer: {}\n'.format(self.person))
        datalist.append('# Abs-Theodolite: {}\n'.format(self.di_inst))
        datalist.append('# Abs-TheoUnit: deg\n')
        datalist.append('# Abs-FGSensor: {}\n'.format(self.fluxgatesensor))
        datalist.append('# Abs-AzimuthMark: {}\n'.format(self.azimuth))
        datalist.append('# Abs-Pillar: {}\n'.format(self.pier))
        datalist.append('# Abs-Scalar: {}\n'.format(self.f_inst))
        datalist.append('# Abs-Temperature: {}C\n'.format(self.t))
        if isinstance(self.inputdate, float):
            try:
                self.inputdate = num2date(self.inputdate)
            except:
                self.inputdate = datetime.now(timezone.utc).replace(tzinfo=None)
        datalist.append('# Abs-InputDate: {}\n'.format(datetime.strftime(self.inputdate,"%Y-%m-%d")))

        datalist.append('Miren:\n')
        datalist.append('{}  {}  {}  {}  {}  {}  {}  {}\n'.format(md1, md1, mu1, mu1, md2, md2, mu2, mu2))
        datalist.append('Positions:\n')
        absst = self.get_abs_distruct()
        for row in absst:
            # modify if f treatment is added
            if not np.isnan(row.hc):
                tt = datetime.strftime(num2date(row.time), "%Y-%m-%d_%H:%M:%S")
                datalist.append('{}  {}  {}  {}\n'.format(tt,row.hc,row.vc,row.res))
        datalist.append('PPM:\n')
        if len(self.ftime) > 0:
            for i, elem in enumerate(self.ftime):
                tt = datetime.strftime(num2date(self.ftime[i]), "%Y-%m-%d_%H:%M:%S")
                datalist.append('{}  {}\n'.format(tt,self.f[i]))
                row = AbsoluteDIStruct()
        datalist.append('Result:\n')

        return datalist


    @deprecated("renamed")
    def getAbsDIStruct(self):
        return self.get_abs_distruct()


    def get_abs_distruct(self):
        """
        DEFINITION:
            convert the DILineStruct to the AbsDataStruct used for calculations

        EXAMPLE:
            abslinestruct.get_abs_distruct()
        """
        try:
            mu1 = self.hc[0]-((self.hc[0]-self.hc[1])/(self.laser[0]-self.laser[1]))*self.laser[0]
            if np.isnan(mu1):
                mu1 = self.hc[0] # in case of laser(vc0-vc1) = 0
        except:
            mu1 = self.hc[0] # in case of laser(vc0-vc1) = 0
        try:
            md1 = self.hc[2]-((self.hc[2]-self.hc[3])/(self.laser[2]-self.laser[3]))*self.laser[2]
            if isnan(md1):
                md1 = self.hc[2] # in case of laser(vc0-vc1) = 0
        except:
            md1 = self.hc[2]
        try:
            mu2 = self.hc[12]-((self.hc[12]-self.hc[13])/(self.laser[12]-self.laser[13]))*self.laser[12]
            if isnan(mu2):
                mu2 = self.hc[12] # in case of laser(vc0-vc1) = 0
        except:
            mu2 = self.hc[12]
        try:
            md2 = self.hc[14]-((self.hc[14]-self.hc[15])/(self.laser[14]-self.laser[15]))*self.laser[14]
            if isnan(md2):
                md2 = self.hc[14] # in case of laser(vc0-vc1) = 0
        except:
            md2 = self.hc[14]

        mu = (mu1+mu2)/2
        md = (md1+md2)/2
        stream = AbsoluteData()
        tmplist = []
        sortlist = []

        headers = {}
        headers['pillar'] = self.pier
        if self.inputdate == '':
            headers['analysisdate'] = str(self.time[0])
        else:
            headers['analysisdate'] = self.inputdate

        # The order needs to be changed according to the file list
        for i, elem in enumerate(self.time):
            if 4 <= i < 12 or 16 <= i < 25:
                row = AbsoluteDIStruct()
                row.time = self.time[i]
                row.hc = self.hc[i] # add the leveling correction
                row.vc = self.vc[i]
                row.res = self.res[i]
                row.mu = mu
                row.md = md
                row.expectedmire = self.azimuth
                row.temp = self.t
                row.person = self.person
                if self.fluxgatesensor == '':
                    row.di_inst = self.di_inst
                else:
                    row.di_inst = self.di_inst +'_'+ self.fluxgatesensor
                row.f_inst = self.f_inst
                tmplist.append(row)

        #print tmplist
        # sortlist
        for idx,elem in enumerate(tmplist):
            if idx < 4:
                sortlist.append(elem)
            if idx == 4:
                sortlist.insert(2,elem)
            if idx == 5:
                sortlist.insert(3,elem)
            if idx == 6:
                sortlist.insert(2,elem)
            if idx == 7:
                sortlist.insert(3,elem)
            if 7 < idx <= 9:
                sortlist.append(elem)
            if idx == 10:
                sortlist.insert(8,elem)
            if idx == 11:
                sortlist.insert(9,elem)
            if idx == 12:
                sortlist.insert(8,elem)
            if idx == 13:
                sortlist.insert(9,elem)
            if idx == 14:
                sortlist.insert(8,elem)
            if idx == 15:
                sortlist.insert(9,elem)

        if len(self.time) < 26:
            sortlist.append(tmplist[8])
        else:
            sortlist.append(tmplist[-1])

        if len(self.ftime) > 0:
            for i, elem in enumerate(self.ftime):
                row = AbsoluteDIStruct()
                row.time = self.ftime[i]
                row.f = self.f[i]
                sortlist.append(row)

        ## Eventually append f values
        for elem in sortlist:
            stream.add(elem)
        return stream


    def save_di(self, path="/tmp"):
        fname = "{}_{}_{}.txt".format(num2date(np.nanmin(np.asarray(self.time))).replace(tzinfo=None), self.pier,
                                  self.stationid).replace(" ", "_").replace(":", "-")
        savelst = self.get_data_list()
        destination = os.path.join(path,fname)
        with open(destination, 'w') as f:
            for line in savelst:
                f.write(f"{line}")


class AbsoluteData(object):
    """
    Creates a list object from input files /url data
    data is organized in columns

    keys are column identifier:
    key in keys: see ABSKEYLIST

    The following application methods are provided:
    - stream.calcabs(key) -- returns stream (with !var2! filled with aic values)

    Supporting internal methods are:
    - self._calcdec() -- calculates dec
    - self._calcinc() -- calculates inc

    """
    def __init__(self, container=None, header=None):
        if container is None:
            container = []
        self.container = container
        if header is None:
            header = {}
        self.header = header
        self.MAGPY_SUPPORTED_ABSOLUTES_FORMATS = ['MAGPYABS','MAGPYNEWABS','JSONABS','AUTODIFABS','AUTODIF','UNKNOWN']
        self.ABSKEYLIST = ['time', 'hc', 'vc', 'res', 'f', 'mu', 'md', 'expectedmire', 'varx', 'vary', 'varz', 'varf', 'var1', 'var2']


    # ----------------
    # Standard functions and overrides for list like objects
    # ----------------

    def add(self, datlst):
        self.container.append(datlst)

    def __str__(self):
        return str(self.container)

    def __repr__(self):
        return str(self.container)

    def __getitem__(self, index):
        return self.container.__getitem__(index)

    def __len__(self):
        return len(self.container)

    def extend(self,datlst,header):
        self.container.extend(datlst)
        self.header = header

    def sorting(self):
        """
        Sorting data according to time (maybe generalize that to some key)
        """
        liste = sorted(self.container, key=lambda tmp: tmp.time)
        return AbsoluteData(liste, self.header)


    def _corrangle(self,angle):
        """
        DESCRIPTION
            make sure that the given input angle in degrees is within [0,360[
        :param angle:
        :return:
        """
        if angle >= 360:
            angle = angle - 360.0
        elif angle < 0:
            angle = angle + 360.0
        return angle


    # ----------------
    # internal methods
    # ----------------

    def _get_max(self, key):
        if not key in self.ABSKEYLIST:
            raise ValueError("Column key not valid")
        elem = max(self, key=lambda tmp: eval('tmp.'+key))
        return eval('elem.'+key)


    def _get_min(self, key):
        if not key in self.ABSKEYLIST:
            raise ValueError("Column key not valid")
        elem = min(self, key=lambda tmp: eval('tmp.'+key))
        return eval('elem.'+key)


    def _get_column(self, key):
        """
        returns an numpy array of selected columns from Stream
        """
        if not key in self.ABSKEYLIST:
            raise ValueError("Column key not valid")

        return np.asarray([eval('elem.'+key) for elem in self])

    def _check_coverage(self,datastream, keys=None):
        """
        DEFINITION:
            check whether variometer/scalar data is available for each time step
            of DI data (within 2* samplingrate diff)
        """
        if not keys:
            keys = ['x','y','z']
        # 1. Drop all data without value
        for key in keys:
            datastream = datastream._drop_nans(key)
        samprate = datastream.samplingrate()
        if not datastream.length()[0] > 1:
            return False

        # 2. Get time column
        timea = datastream.ndarray[0].astype(datetime64)

        # 3. Get time column of DI data
        timeb = np.asarray([num2date(el.time).replace(tzinfo=None) for el in self]).astype(datetime64)
        # 4. search
        # corrected in version 0.9.9
        indtia = [idx for idx, el in enumerate(timeb) if np.min(np.abs((timea-el)/1000000.).astype(float64))/((samprate)*2) <= 1.]
        if not len(indtia) == len(timeb):
            print ("_check_coverage: timesteps of scalar data are off by more than twice the sampling rate from DI measurements")
            return False

        return True

    def _insert_function_values(self, function, funckeys=None, validkeys=DataStream().KEYLIST, offset=0.0, debug=False):
        """
        DEFINITION:
            Add a function-output to the selected values of the data stream -> e.g. get baseline

        PARAMETERS:
        kwargs:
            - funckeys          (KEYLIST) (default = 'x','y','z','f')
            - offset            (float) provide an offset for keys

        USED BY:

        RETURNS:
            stream with function values added

        EXAMPLE:
            abstream._calcdec()
        """
        if not funckeys:
            funckeys = function[-1]

        for elem in self:
            # check whether time step is in function range
            if function[1] <= elem.time <= function[2]:
                functime = (elem.time-function[1])/(function[2]-function[1])
                #if debug:
                #    print ("Inserting at {}:".format(num2date(functime)))
                for key in funckeys:
                    if not key in validkeys[1:15]:
                        raise ValueError("Column key not valid")
                    fkey = 'f'+key
                    if fkey in function[0]:
                        try:
                            newval = float(function[0][fkey](functime)) + offset
                        except:
                            newval = np.nan
                        exec('elem.var'+key+' = newval')
                    #if debug:
                    #    print (" -> key {}: {}".format(key,newval))
                    else:
                        pass
            else:
                pass

        return self

    def _calcdec(self, **kwargs):
        """
        DEFINITION:
            Calculates declination values from input.
            Returns results in terms of linestruct

        PARAMETERS:
        kwargs:
            - annualmeans:      (list of floats) a list providing Observatory specific annual mean values in x,y,z nT (e.g. [20000,1000,43000])
            - xstart:           (float) strength of the north component in nT
            - ystart:           (float) default 0.0 - strength of the east component in nT
                       - if variometer is hdf oriented then xstart != 0, ystart = 0
                       - if baselinecorrected data is used (or dIdD) use xstart = 0, ystart = 0
            - hbasis            (float) the basevalue of the variometers x-axis (corresponding to mag North for HEZ)
                                       (corresponding to geo North for XYZ)
            - ybasis            (float) the basevalue of the variometers y-axis (0 for mag North for HEZ, non-zero for XYZ)
            - deltaD:           (float) - default 0.0 - eventual correction factor for declination in degree
            - usestep:          (int) use first, second or both of successive measurements (e.g. autodif requires usestep=2)
            - iterator:         (int) switch of loggerabs (e.g. vario not present) in case of iterative approach
            - scalevalue:       (list of floats) scalevalues for each component (e.g. default = [1,1,1])
            - debugmode:        (bool) activate additional debug output

        USED BY:

        RETURNS:
            - line:             (LineStruct)  containing time, y = dec, typ = 'idff',
                                        var1 = s0d, var2 = deH, var3 = epZD, t2 = mirediff,
                                        str1 = person, str2 = di_inst, str3 = f_inst

        EXAMPLE:
            absstream._calcdec()
        """

        xstart = kwargs.get('xstart')
        ystart = kwargs.get('ystart')
        ybasis = kwargs.get('ybasis')
        hbasis = kwargs.get('hbasis')
        hstart = kwargs.get('hstart')
        deltaD = kwargs.get('deltaD')
        usestep = kwargs.get('usestep')
        scalevalue = kwargs.get('scalevalue')
        iterator = kwargs.get('iterator')
        debugmode = kwargs.get('debugmode')
        annualmeans = kwargs.get('annualmeans')
        meantime = kwargs.get('meantime')
        xyzorient = kwargs.get('xyzorient') # True or False
        residualsign = kwargs.get('residualsign') # Orientation of residual measurement - inline or opposite

        ang_fac = 1
        if not deltaD:
            deltaD = 0.0
        if not scalevalue:
            scalevalue = [1.0,1.0,1.0]
        if not annualmeans:
            annualmeans = [0.0,0.0,0.0]
        if not xstart:
            xstart = annualmeans[0]
        if not ystart:
            ystart = annualmeans[1]
        if not usestep:
            usestep = 0
        if not iterator:
            iterator = 0
        if not debugmode:
            debugmode = False
        if not meantime:
            # If meantime is False, then the calculation will be transformed to t0
            # which is the time of the first measurement (conform with spreedsheet)
            determinationindex = 0
        if not residualsign and not residualsign in [1,-1]:
            residualsign = 1

        determinationindex = 0
        scale_x = scalevalue[0]
        scale_y = scalevalue[1]
        scale_z = scalevalue[2]

        if not hstart:
            hstart = np.sqrt((xstart)**2 + (ystart)**2)
        if not hbasis:
            hbasis = hstart
        if not ybasis:
            ybasis = 0.0

        if debugmode:
            print (" Running declination calc: xstart={},ystart={},hstart={},hbasis={},ybasis={}".format(xstart, ystart, hstart, hbasis, ybasis))

        #drop NANs from input stream - positions
        expmire = self._get_column('expectedmire')
        expmire = [float(elem) for elem in expmire]
        expmire = np.mean([elem for elem in expmire if not isnan(elem)])
        poslst = [elem for elem in self if not isnan(elem.hc) and not isnan(elem.vc)]

        if debugmode:
            print (" Expected Mire: expmire={}".format(expmire))

        try:
            if len(poslst) < 1:
                loggerabs.warning("_calcdec: could not identify measurement positions - aborting")
                return LineStruct()
        except:
            loggerabs.warning("_calcdec: could not assign measurement positions - aborting")
            return LineStruct()

        # Check that: Should be correct if the order of input values is correct
        miremean = np.mean([poslst[1].mu,poslst[1].md]) # fits to mathematica
        # relative mire position of theo is either miremean +90 or -90
        # if sheet is exactly used then it would be easy... however sensor down first measurements have not frequently be used...
        if debugmode:
            print (" Meanmire: miremean={}".format(miremean))

        if poslst[1].mu-5 < (miremean-90.0) < poslst[1].mu+5:
            mireval = miremean-90.0
        else:
            mireval = miremean+90.0
        if debugmode:
            print (" Corrected Meanmire: mireval={}".format(mireval))
        mirediff = self._corrangle(expmire - mireval)
        if debugmode:
            print (" Mirediff by corrangle(expmire-mireval: mirediff={}".format(mirediff))

        loggerabs.debug("_calcdec: Miren: %f, %f, %f" % (expmire, poslst[1].mu, miremean))

        # -- Get mean values for x and y
        # ------------------------------
        meanx, meany, meanz = 0.0,0.0,0.0
        xcol, ycol, zcol, tlist = [],[],[],[]
        for idx, elem in enumerate(poslst):
            if idx < 8:
                if not isnan(elem.varx):
                    xcol.append(elem.varx)
                if not isnan(elem.vary):
                    ycol.append(elem.vary)
                if not isnan(elem.varz):
                    zcol.append(elem.varz)
            tlist.append(elem.time)
        if len(xcol) > 0:
            meanx = np.mean(xcol)
        if len(ycol) > 0:
            meany = np.mean(ycol)
        if len(zcol) > 0:
            meanz = np.mean(zcol)

        nr_lines = len(poslst)
        dl1,dl2,dl2tmp,variocorr, variohlst = [],[],[],[],[] # temporary lists

        if debugmode:
            print ("Declination means for xvar and yvar:", meanx, meany)
            print (ycol)

        # -- check, whether inclination and declination values are present:
        # ------------------------------
        if nr_lines < 9:
            linecount = int(nr_lines)
        else:
            linecount = int((nr_lines-1)/2.)

        # -- Determine poslst index closest to mean time
        # ----------------------------------------------
        # Usage of meantime deviates from spreedsheet tool
        if meantime:
            avtime = np.mean(np.asarray(tlist))
            postime = np.asarray([el.time for el in poslst][:linecount])
            # get the index of postime with least difference to average time
            mindiff = np.abs(postime-avtime)
            determinationindex = np.where(mindiff < np.min(mindiff)+0.000001)[0]
            #print("determinationindex", determinationindex)
            #print ("Time", num2date(poslst[determinationindex].time))

        # -- cycling through declinations measurements:
        # ------------------------------
        for k in range(linecount):
            if (poslst[k].hc > poslst[k].vc):
                val = poslst[k].hc-poslst[k].vc
            else:
                val = poslst[k].hc + (360) - poslst[k].vc
            # removed 2015: signum = np.sign(np.tan(val))
            if k in [0,1,4,5]:
               signum = 1.0
            else:
               signum = -1.0
            #signum = -1.0 # hungarian sheet - should not be correct - tested with workshop data

            if hstart == 0:
                rescorr = 0
            else:
                #print ("Check: ", hbasis, poslst[k].varx, ybasis, poslst[k].vary)
                if not isnan(poslst[k].varx) and not isnan(poslst[k].vary):
                    rescorr = signum*np.arcsin( residualsign*poslst[k].res / np.sqrt( (hbasis+scale_x*poslst[k].varx)**2 + (ybasis+scale_y*poslst[k].vary)**2 ) )
                else:
                    rescorr = signum*np.arcsin( residualsign*poslst[k].res / hstart)
            if xstart+poslst[k].varx == 0 or isnan(poslst[k].varx) or isnan(hbasis):
                varco = 0.0
            else:
                varco = np.arctan((ybasis+scale_y*poslst[k].vary)/(hbasis+scale_x*poslst[k].varx))
            variocorr.append(varco)
            # a1 = hc + asin(res1/sqrt[ (hstart+vx)**2 + vy**2 ])*180/Pi - atan( vy/(hstart+vx) )*180/Pi
            #print "TESTVALUE:", poslst[k].hc*np.pi/(180.0)*200/np.pi, rescorr*200/np.pi, varco*200/np.pi, hbasis
            dl1val = poslst[k].hc*np.pi/(180.0) + rescorr - variocorr[k]
            dl1org = dl1val
            # make sure that the resulting angle is between 0 and 2*pi
            if dl1val > 2*np.pi:
                dl1val -= 2*np.pi
            elif dl1val < 0:
                dl1val += 2*np.pi
            dl1.append(dl1val)
            loggerabs.debug("_calcdec: Horizontal angles: %f, %f, %f" % (poslst[k].hc, rescorr, variocorr[k]))
            if debugmode:
                print ("_calcdec: Horizontal angles: %f, %f, %f; dl1org: %f, dl1: %f" % (poslst[k].hc, rescorr*180.0/np.pi, variocorr[k]*180.0/np.pi, dl1org, dl1[k]))

        try:
            if len(dl1) < 8:
                loggerabs.warning("_calcdec: horizontal angles could not be assigned - aborting")
                return LineStruct()
        except:
            loggerabs.warning("_calcdec: horizontal angles could not be assigned - aborting")
            return LineStruct()

        # use selected steps, default is average....
        for k in range(0,7,2):
            if usestep == 1:
                dl2mean = dl1[k]
            elif usestep == 2:
                dl2mean = dl1[k+1]
            else:
                try:
                    dl2mean = np.mean([dl1[k],dl1[k+1]])
                except:
                    loggerabs.error("_calcdec:  {} : Data missing: check whether all fields are filled".format(poslst[0].time))
                    dl2mean = 0
                    pass
            dl2tmp.append(dl2mean)
            loggerabs.debug("_calcdec: Selected Dec: %f" % (dl2mean*180/np.pi))
            # New ... modify range to be between 0 and pi
            if dl2mean > 2*np.pi:
                dl2mean -= 2*np.pi
            if dl2mean < np.pi:
                dl2mean += np.pi/2
            else:
                dl2mean -= np.pi/2
            #dl2tmp.append(dl2mean)
            dl2.append(dl2mean)

        decmean = np.mean(dl2)*180.0/np.pi - 180.0

        loggerabs.debug("_calcdec:  Mean Dec: %f, %f" % (decmean, mirediff))

        #Initialize HC if no input in this column = 0
        try:
            if (poslst[8].hc == 0 or poslst[8].hc == 180):
                for k in range(8,16):
                    self[k].hc += decmean
        except:
            if iterator == 0:
                loggerabs.warning("_calcdec: %s : No inclination measurements available"% num2date(poslst[0].time).replace(tzinfo=None))
            pass

        # Stupid test of validity - needs to be removed if input order is finally correct
        if 90 > self._corrangle(mirediff + decmean) >= 0:
            corrfac = 0.0
        elif  360 >= self._corrangle(mirediff + decmean) > 270:
            corrfac = 0.0
        else:
            corrfac = 180.0
            mirediff = self._corrangle(mirediff - 180.0)

        if (np.max(dl2)-np.min(dl2))>0.1:
            if iterator == 0:
                loggerabs.error('_calcdec: %s : Check the horizontal input of absolute data (or xstart value)' % num2date(poslst[0].time).replace(tzinfo=None))

        # see also IM technical manual (5.0.0), page 45, formula 1c:
        dec_baseval = self._corrangle(decmean + mirediff + deltaD)
        dec = self._corrangle(decmean + mirediff + variocorr[determinationindex]*180.0/np.pi + deltaD)
        # dec is used for return (fist value), meandec is used for basevalue at mean of all dec measus
        meandec = self._corrangle(decmean + mirediff + np.mean(variocorr)*180.0/np.pi + deltaD)

        loggerabs.debug("_calcdec:  All (dec: %f, decmean: %f, mirediff: %f, variocorr: %f, delta D: %f and ang_fac: %f, hstart: %f): " % (dec, decmean, mirediff, variocorr[determinationindex]*180.0/np.pi, deltaD, ang_fac, hstart))
        if debugmode:
            print ("_calcdec:  All (dec: %f, decmean: %f, mirediff: %f, variocorr: %f, delta D: %f and ang_fac: %f, hstart: %f): " % (dec, decmean, mirediff, variocorr[determinationindex], deltaD, ang_fac, hstart))

        if not hstart == 0:
            s0d = (dl2tmp[0]-dl2tmp[1]+dl2tmp[2]-dl2tmp[3])/4*hstart
            deH = (-dl2tmp[0]-dl2tmp[1]+dl2tmp[2]+dl2tmp[3])/4*hstart
            if debugmode:
                print ("_calcdec:  collimation angle (dl2tmp): %f, %f, %f, %f; hstart: %f" % (dl2tmp[0],dl2tmp[1],dl2tmp[2],dl2tmp[3],hstart))
            loggerabs.debug("_calcdec:  collimation angle (dl2tmp): %f, %f, %f, %f; hstart: %f" % (dl2tmp[0],dl2tmp[1],dl2tmp[2],dl2tmp[3],hstart))
            if dl2tmp[0]<dl2tmp[1]:
                epZD = (dl2tmp[0]-dl2tmp[1]-dl2tmp[2]+dl2tmp[3]+2*np.pi)/4*hstart
            else:
                epZD = (dl2tmp[0]-dl2tmp[1]-dl2tmp[2]+dl2tmp[3]-2*np.pi)/4*hstart
        else:
            s0d = np.nan
            deH = np.nan
            epZD = np.nan

        resultline = LineStruct()
        try:
            resultline.time = avtime # if meantime == True
        except:
            resultline.time = poslst[determinationindex].time
        resultline.y = dec
        if dec_baseval > 270: # for display
            dec_baseval = dec_baseval-360.
        resultline.dy = dec_baseval
        if xyzorient:
            resultline.dx = hbasis
            resultline.dy = ybasis
        resultline.typ = 'idff'
        resultline.var1 = s0d
        resultline.var2 = deH
        resultline.var3 = epZD
        resultline.var5 = determinationindex
        resultline.t1 = poslst[determinationindex].temp
        resultline.t2 = mirediff
        # general info:
        resultline.str1 = poslst[0].person
        resultline.str2 = poslst[0].di_inst
        resultline.str3 = str(expmire)

        return resultline, meanx, meany, meandec


    def _h(self, f, inc):
        return f * np.cos(inc*np.pi/(180.0))


    def _z(self, f, inc):
        return f * np.sin(inc*np.pi/(180.0))


    def _calcinc(self, linestruct, **kwargs):
        """
        DEFINITION:
            Calculates inclination values from input.
            Need input of a LineStruct Object containing the results of _calcdec

        PARAMETERS:
        variable:
            - line              (LineStruct) a line containing results from _calcdec()
        kwargs:
            - annualmeans:      (list of floats) a list providing Observatory specific annual mean values in x,y,z nT (e.g. [20000,1000,43000])
            - incstart          (float) - default 45.0 - inclination value in deg
            - deltaI:           (float) - default 0.0 - eventual correction factor for inclination in degree
            - usestep:          (int) use first, second or both of successive measurements (e.g. autodif requires usestep=2)
            - iterator:         (int) switch of loggerabs (e.g. vario not present) in case of iterative approach
            - scalevalue:       (list of floats) scalevalues for each component (e.g. default = [1,1,1])
            - debugmode:        (bool) activate additional debug output -- !!!!!!! removed and replaced by loggin !!!!!

        USED BY:

        REQUIRES:
            a LineStruct element as returned by _calcdec

        RETURNS:
            - line,xstart,ystart:       (LineStruct, float, float) last two for optimzing calcdec in an iterative process
                                        LineStruct is extended by x = inc, typ = 'idff', z=fstart, f=fstart,
                                        var4 = s0i, var5 = epzi, dx = basex, dy = basey,
                                        dz = basez, df = deltaf, t2 = calcscaleval

        EXAMPLE:
            abstream._calcinc()
        """

        incstart = kwargs.get('incstart')
        scalevalue = kwargs.get('scalevalue')
        deltaI = kwargs.get('deltaI')
        iterator = kwargs.get('iterator')
        debugmode = kwargs.get('debugmode')
        annualmeans = kwargs.get('annualmeans')
        usestep = kwargs.get('usestep')
        decmeanx =  kwargs.get('decmeanx')
        decmeany =  kwargs.get('decmeany')
        variocorrold = kwargs.get('variocorrold')
        xyzorient = kwargs.get('xyzorient') # True or False
        residualsign = kwargs.get('residualsign') # Orientation of residual measurement - inline or opposite

        ang_fac = 1
        if not scalevalue:
            scalevalue = [1.0,1.0,1.0]
        if not annualmeans:
            annualmeans = [0.0,0.0,0.0]
        if not incstart:
            incstart = 45.0
        if not iterator:
            iterator = 0
        if not debugmode:
            debugmode = False
        if not usestep:
            usestep = 0
        if not residualsign and not residualsign in [1,-1]:
            residualsign = 1

        determinationindex = int(linestruct.var5)
        variocorr = []
        meanf = 0

        calcscaleval = 999.
        scale_x = scalevalue[0]
        scale_y = scalevalue[1]
        scale_z = scalevalue[2]

        # Initialize variometer and scalar instruments .... maybe better not here
        variotype = 'None'
        scalartype = 'None'
        str4 = 'None'  # Hold a value regarding the used F: Fabs (in absolute file),
                       # Fext (from file), Fannual (from provided annual means), or None

        S0I1 = 0.
        S0I2 = 0.
        S0I3 = 0.
        EZI1 = 0.
        EZI2 = 0.
        EZI3 = 0.

        # -- Get the variometer and scalar means from then absolute file
        # --------------------------------------------------------------
        #drop NANs from input stream - positions and get means
        poslst = [elem for elem in self if not isnan(elem.hc) and not isnan(elem.vc)]
        fcol = self._get_column('f')
        mflst = [elem for elem in fcol if not isnan(elem)]
        fvcol = self._get_column('varf')
        mfvlst = [elem for elem in fvcol if not isnan(elem)]
        # Select variometer readings for existing intensity data
        variox, varioy, varioz, flist, fvlist, dflist  = [],[],[],[],[],[]
        for elem in self:
            if len(mflst) > 0 and not isnan(elem.f):
                # F values provided in DI file
                flist.append(elem.f)
                if not isnan(elem.varz) and not isnan(elem.vary) and not isnan(elem.varx):
                    variox.append(scale_x*elem.varx)
                    varioy.append(scale_y*elem.vary)
                    varioz.append(scale_z*elem.varz)
                if not isnan(elem.varf):
                    fvlist.append(elem.varf)
                    dflist.append(elem.f-elem.varf)
            elif len(mflst) == 0 and len(mfvlst) > 0:
                # F values taken from separate scalar data
                if not isnan(elem.varz) and not isnan(elem.vary) and not isnan(elem.varx):
                    variox.append(scale_x*elem.varx)
                    varioy.append(scale_y*elem.vary)
                    varioz.append(scale_z*elem.varz)
                if not isnan(elem.varf):
                    fvlist.append(elem.varf)

        # ###################################
        # determine average F value
        #     1. check Absolute file - no delta F -> use delta F from abs file
        #     2. check Scalar path
        #     3. check provided annual means
        #     4. basevalue is calculated at a specific time defined by determinationindex
        #        -> According to Juerges excel sheet only D is determined at t0
        #        -> I and F are averages within time range of DI meas
        #     5. The current version is based on the DTU Excel sheet
        # ###################################

        nr_lines = len(poslst)

        if len(mflst)>0:
            if debugmode:
                print ("Using primary F values contained in absolute file")
            str4 = "Fabs"
            meanf = np.mean(flist)
            loggerabs.debug("_calcinc: Using F from Absolute files")
        elif len(mfvlst)>0:
            if debugmode:
                print ("Using primary F values contained in separate file")
            str4 = "Fext"
            #if len(fvlist) > 16: ## Select values during inclination measurement to be coherent with excel sheet
            #    fvlist=fvlist[8:16]
            meanf = np.mean(fvlist)
            loggerabs.debug("_calcinc: Using F from provided scalar path")
        else:
            loggerabs.warning("_calcinc: No F measurements found - using annual means ...")
            meanf = np.sqrt(annualmeans[0]*annualmeans[0] + annualmeans[1]*annualmeans[1] + annualmeans[2]*annualmeans[2])
            if not meanf == 0:
                str4 = "Fannual"
            #return emptyline, 20000.0, 0.0


        if debugmode:
            print ("Mean values for x,y,z,f determined from lengths:", len(variox),len(varioy),len(varioz),len(flist),len(fvlist))

        # ###################################
        # Getting variometer data for F values
        #      proper correction requires scalar values
        # ###################################

        if len(variox) == 0:
            #if iterator == 0:
            #    loggerabs.warning("_calcinc: %s : no variometervalues found" % num2date(self[0].time).replace(tzinfo=None))
            # set means to zero...
            meanvariox = 0.0
            meanvarioy = 0.0
            meanvarioz = 0.0
        else:
            # Use whatever is there, should never be the case actually
            meanvariox = np.mean(variox)
            meanvarioy = np.mean(varioy)
            meanvarioz = np.mean(varioz)

        if debugmode:
             print ("Getting delta F", np.mean(flist)-np.mean(fvlist),np.mean(dflist))

        #drop NANs from input stream - fvalues
        if len(dflist) > 0:
            deltaF = np.mean(dflist)
        elif len(flist) > 0 and len(fvlist) == len(flist):
            deltaF = np.mean(flist)-np.mean(fvlist)
        else:
            deltaF = np.nan

        # -- Start with the inclination calculation
        # --------------------------------------------------------------
        #I0Diff1,I0Diff2,xDiff1,zDiff1,xDiff2,zDiff2= 0,0,0,0,0,0
        I0list, ppmval,testlst,tlist = [],[],[],[]
        xvals, yvals, zvals =  [],[],[]
        cnt = 0
        mirediff = linestruct.t2

        # Check that mirediff is correct
        # get the correct hc values and replace 0 and 180 degree with the correct data

        # If only D measurements are available then no suitable estimate of H can be obtained
        # Therefore we use an arbitrary start value -- Testing of the validity of this approach needs to be conducted
        if nr_lines < 16: # only declination measurements available so far
            return linestruct, 20000.0, 0.0

        # - Now cycle through inclination steps and apply residuum correction
        for k in range(int((nr_lines-1)/2.),int(nr_lines)):
            val = poslst[k].vc
            try:
                # Calculate the mean variations during the I measurements -> Used for offset calc
                xvals.append(scale_x*poslst[k].varx)
                yvals.append(scale_y*poslst[k].vary)
                zvals.append(scale_z*poslst[k].varz)
                if meanf == 0:
                    ppmtmp = np.nan
                else:
                    # correctness the following line requires that meanf is determined in the same time range as meanvario comps
                    # other sheets only take meanf and variomeans only during the respective cycle - with constant delta F
                    ppmtmp = meanf + (scale_x*poslst[k].varx - meanvariox)*np.cos(incstart*np.pi/180.) + (scale_z*poslst[k].varz - meanvarioz)*np.sin(incstart*np.pi/180.) + ((scale_y*poslst[k].vary)**2-(meanvarioy)**2)/(2*meanf)
                # old version ---> ppmtmp = np.sqrt(xtmp**2 + ytmp**2 + ztmp**2)
                if isnan(ppmtmp):
                    ppmval.append(meanf)
                else:
                    ppmval.append(ppmtmp)
                tlist.append(poslst[k].time)
            except:
                ppmval.append(meanf)
            if not ppmval[cnt] == 0:
                rcorri = np.arcsin(residualsign*poslst[k].res/ppmval[cnt])
            else:
                rcorri = 0.0
            if poslst[k].vc > (poslst[k].hc + mirediff):
                quad = poslst[k].vc - (poslst[k].hc + mirediff)
            else:
                quad = poslst[k].vc + 360.0 - (poslst[k].hc + mirediff)
            # The signums are essential for the collimation angle calculation
            # test_di: also checks regarding correctness for different location/angles
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            signum1 = np.sign(-np.cos(quad*np.pi/180.0))
            signum2 = np.sign(np.sin(quad*np.pi/180.0))
            if signum2>0:
                PiVal=2*np.pi
            else:
                PiVal=np.pi

            # Starting new approach here
            if cnt in [0,1,6,7]:
                signum2 = 1.0
                signum3 = 1.0
            else:
                signum2 = -1.0
                signum3 = -1.0
            #signum2 = 1.0 # i.e. hungarian sheet - is this correct?
            I0 = 0
            sigdf = 1.0
            if incstart < 0: ###### check here
                signum2 = -1.*signum2
            if cnt in [0,1]:
                I0 = (poslst[k].vc*np.pi/180.0 - signum2*rcorri)
                sigdf = 1.0
            elif cnt in [2,3]:
                I0 = (poslst[k].vc*np.pi/180.0 - signum2*rcorri) - np.pi
                sigdf = -1.0
            elif cnt in [4,5]:
                I0 = 2*np.pi - (poslst[k].vc*np.pi/180.0 - signum2*rcorri)
                sigdf = 1.0
            elif cnt in [6,7]:
                I0 = np.pi - (poslst[k].vc*np.pi/180.0 - signum2*rcorri)
                sigdf = -1.0

            # S0I
            # (northern =-(H32-H33+H34-H35)/4/200*PI()*K35-((F15-F16+F17-F18)*SIN(H40/200*PI())-(H15-H16+H17-H18)*COS(H40/200*PI()))/4
            # southern is identical
            # test with different azimuth marks from one location
            # EZ0I
            #( northern =((-H32-H33+H34+H35)/4/200*PI()-((F15+F16-F17-F18)*SIN(H40/200*PI())-(H15+H16-H17-H18)*COS(H40/200*PI()))/(4*K35))*K38
            #( southern =((-H32-H33+H34+H35)/4/200*PI()-((F15+F16-F17-F18)*SIN(H40/200*PI())-(H15+H16-H17-H18)*COS(H40/200*PI()))/(4*K35))*K38

            if k < nr_lines-1:
                I0list.append(I0)
                # new
                S0I1 += sigdf*I0
                S0I2 += sigdf*poslst[k].varx
                S0I3 += sigdf*poslst[k].varz
                EZI1 += -signum3*sigdf*I0
                EZI2 += signum3*sigdf*poslst[k].varx
                EZI3 += signum3*sigdf*poslst[k].varz
            else:
                scaleangle = poslst[k].vc
                minimum = 10000
                calcscaleval = 999.0
                for n in range(int((nr_lines-1)/2.),int(nr_lines-1)):
                    rotation = np.abs(scaleangle - poslst[n].vc)
                    if 0.03 < rotation < 0.5: # Only analyze scale value if last step (17) deviates between 0.03 and 0.5 degrees from any other inclination value
                        #=(-SIN(B37*PI()/200)*(F20-F19)/K35+COS(B37*PI()/200)*(H20-H19)/K35)*200/PI()
                        if mean(ppmval) == 0:
                            fieldchange = np.nan
                        else:
                            fieldchange = (-np.sin(np.mean(I0list))*(poslst[n].varx-poslst[k].varx)/mean(ppmval) + np.cos(np.mean(I0list))*(poslst[n].varz-poslst[k].varz)/mean(ppmval))*180/np.pi
                        deltaB = rotation+fieldchange
                        deltaR = np.abs(poslst[n].res-poslst[k].res)
                        minimum = rotation
                        if (deltaR == 0):
                            calcscaleval = 999.0
                        else:
                            calcscaleval = mean(ppmval) * deltaB/deltaR * np.pi/180
                        loggerabs.debug("_calcinc: Scalevalue calculation in calcinc - Fieldchange: %f, Scalevalue: %f, DeltaR: %f" % (fieldchange,calcscaleval,deltaR))

            cnt += 1

        i1list,i1tmp = [],[]
        #print("Count it", I0list, np.std(I0list))

        for k in range(0,7,2):
            if usestep == 1:
                i1mean = I0list[k]
            elif usestep == 2:
                i1mean = I0list[k+1]
            else:
                i1mean = np.mean([I0list[k],I0list[k+1]])
            i1tmp.append(i1mean)
            i1list.append(i1mean)

        # Unlike the excel MagPy first uses all individual components and averages afterwards. The Sum
        # for collimation angle needs to be divided by two

        S0I1 = S0I1/2.
        S0I2 = S0I2/2.
        S0I3 = S0I3/2.
        EZI1 = EZI1/2.
        EZI2 = EZI2/2.
        EZI3 = EZI3/2.

        #i1list = [np.abs(elem) for elem in i1list]
        #print ("Inclist", i1list, deltaI)

        # Variometer correction to start time is missing for f value and inc ???
        inc = np.mean(i1list)*180.0/np.pi + deltaI

        if isnan(inc): # if only dec measurements are available
            inc = 0.0
        # Dec is already variometer corrected (if possible) and transferred to time[0]

        if inc > 90:
            inc = inc-180

        # S0I1 is calculated with I0. I0 is obtained from each circle, recalculated already towards the intial circle. Thereby it is normalized towards the northern hemisphere.
        # Please note: this differs from the Excel sheet of Juergs, yet it is apparently correct and can be tested by a zero hypothesis:
        # Use a measurement without residual, calculate S0 for D and I and add -S0 into D and I lines. When recalculating
        # this file, the S0 values should now be zero.
        # S0I2 and S0I3 are not affected.
        # A review is highly welcome

        if inc < 0:
            S0I1 = -S0I1

        # determine best F
        # Please note: excel sheet averages all Variometer corrected values:
        # meanf contains the average F value off all F provided along with DI file or data from scalar file
        #          meanf is NOT representative for the time range of the measurement
        # ppmval list consists of: 1) ppm values corrected for variation if values contained in abs file
        #                          2) ppm values corrected for variation if values contained in external file
        #                          3) F from provided annual means
        # Note: these averages are used for determining collimation angels si0 and epzi

        if len(ppmval) > 8: # drop value from scale measurement
            selppmval = ppmval[:8]
            meanf =  mean(selppmval)
        if len(xvals) > 8: # drop value from scale measurement
            xvals = xvals[:8]
            yvals = yvals[:8]
            zvals = zvals[:8]

        #avcorrf =  mean(ppmval) # definitely not meanf, evetually use ppmval[0] to pick first time step
        tmpH = self._h(meanf, inc)
        tmpZ = self._z(meanf, inc)
        dec = linestruct.y
        if xyzorient:
            # for xyz use the average variocorrected declination value from the hc measurements
            dec2 = variocorrold
        else:
            # for hdz base value and dec are determined at the initial time step
            dec2 = dec
        tmpX = self._h(tmpH, dec2)
        tmpY = self._z(tmpH, dec2)

        if xyzorient and debugmode:
            print ("XYZ technique: dec(starttime)={}, dec2(average hc)={}, inv(average vc)={}, tmpX={}, tmpY={}, tmpZ={}".format(dec, dec2, inc, tmpX, tmpY, tmpZ))
            print ("Control (vector sum vs mean F):", np.sqrt(tmpX**2+tmpY**2+tmpZ**2), meanf)

        # check for inclination error in file inc
        #   -- the following part may cause problems in case of close to polar positions and locations were X is larger than Y
        if (90-inc) < 0.1:
            loggerabs.warning('_calcinc: %s : Inclination warning... neglect if you are on the southern hemisphere. If not check your vertical measurements. inc = %f, mean F = %f' % (num2date(self[0].time).replace(tzinfo=None), inc, meanf))

        # ###################################################
        # #####      Calculating Collimation angles from average intensity and inc
        # ###################################################

        if not meanf == 0:
            ### Please Note: There is an observable difference in the first term of s0i calculation in
            ### comparison to Juergs excel sheet
            ### the reason is unclear S0I1 is perfectly OK and meanf as well
            ### S0I1 however is usually very small wherefore rounding effects play an important role
            ## The underlying formula can be found in Janukowski and Sucksdorf, 1996
            s0i = -S0I1/4.*meanf -  (S0I2*np.sin(inc*np.pi/180.) - S0I3*np.cos(inc*np.pi/180.))/4.
            epzi = (EZI1/4. - ( EZI2*np.sin(inc*np.pi/180.) - EZI3*np.cos(inc*np.pi/180.) )/(4.*meanf))* (meanf*np.sin(inc*np.pi/180.))
        else:
            loggerabs.warning("_calcinc: %s : no intensity measurement available - you might provide annual means"  % num2date(self[0].time).replace(tzinfo=None))
            s0i, epzi = np.nan,np.nan
            fstart, deltaF = np.nan,np.nan
            xstart = 0.0 ## arbitrary
            ystart = 0.0

        # ###################################################
        # #####      Calculating offsets (Base values in Excel sheet)
        # ###################################################

        # By default:
        # Running calc according to script: => offsets are determined for the average I measurement part
        # Please note: as offsets are calculated from the averages of all measurements
        #              the final result is not representative for t0
        #              but actually for average time of measurements....

        if not decmeanx:
            if debugmode:
                print (" No variationmean for X from calcdec ")
            decmeanx = mean(xvals)
        if not decmeany:
            if debugmode:
                print (" No variationmean for Y from calcdec ")
            decmeany = mean(yvals)

        # =RUNDEN(WURZEL(K37^2-(MITTELWERT(G15:G18))^2)-MITTELWERT(F15:F18);1)
        # asuming x coressponds to magnetic north direction
        # h2 = (x+hbase)2+y2 -> sqrt(h2-y2) - x = hbase
        x_adder = 0.0
        y_adder = 0.0
        if len(xvals) > 0:
            if tmpH**2 - mean(yvals)**2 < 0: # if no scalar data is available
                h_adder = np.nan
                z_adder = np.nan
            elif xyzorient:
                # temX,Y are determined based on F during inc cycle, variationmeans from dec cycle, not perfect but reasonable - changed for version 1.1.4 to variationcycle of inc as sometimes not reasonable
                if debugmode:
                    print ("Variation correction: varx={}, varz={} (determined from vc); vary={} (determined from hc)".format(np.mean(xvals),np.mean(zvals),decmeany))
                    print (tmpX,np.mean(xvals))
                    print (tmpY,decmeany)
                    print (tmpZ,np.mean(zvals))
                x_adder = tmpX-np.mean(xvals)
                y_adder = tmpY-decmeany #mean(yvals)
                z_adder = tmpZ-np.mean(zvals)
                h_adder = x_adder
            else:
                h_adder = np.sqrt(tmpH**2 - (mean(yvals))**2) - mean(xvals)
                z_adder = tmpZ-mean(zvals)
            if debugmode:
                print(" determined offsets:", h_adder, x_adder, y_adder, z_adder, decmeanx, mean(xvals), decmeany, mean(yvals), mean(zvals), xyzorient)
        else:
            #print ("No variometer data - using estimated H and Z")
            x_adder = tmpX
            y_adder = tmpY
            h_adder = tmpH
            z_adder = tmpZ

        # ###################################################
        # #####      Recalculating field values at time k=0
        # ###################################################
        #print ("Time", determinationindex, scale_x, h_adder, scale_y)
        if not np.isnan(poslst[determinationindex].varx) and not np.isnan(h_adder):
            hstart = np.sqrt((scale_x*poslst[determinationindex].varx + h_adder)**2 + (scale_y*poslst[determinationindex].vary + y_adder)**2)
            xstart = hstart * np.cos ( dec *np.pi/(180.0) )
            ystart = hstart * np.sin ( dec *np.pi/(180.0) )
            zstart = scale_z*poslst[determinationindex].varz + z_adder
            fstart = np.sqrt(hstart**2 + zstart**2)
            #print ("Difference:", fstart, self[determinationindex].varf, meanf)
            inc = np.arctan(zstart/hstart)*180.0/np.pi
        else:
            variotype = 'None'
            hstart = tmpH
            zstart = tmpZ
            xstart = tmpH * np.cos ( dec *np.pi/(180.0) )
            ystart = tmpH * np.sin ( dec *np.pi/(180.0) )
            fstart = np.sqrt(hstart**2 + zstart**2)

        # Directional base values -- would avoid them
        basex = h_adder
        basey = y_adder
        basez = z_adder

        if debugmode:
            print ("Bases:",basex,basey,basez)
        def coordinate(vec):
            #vec = [64.2787283564,3.76836507656,48459.6180931]
            dc = vec[1]*np.pi/(180.)
            ic = vec[0]*np.pi/(180.)
            x = vec[2]*np.cos(dc)*np.cos(ic)
            y = vec[2]*np.sin(dc)*np.cos(ic)
            z = vec[2]*np.sin(ic)
            return [x,y,z]

        # The following was tested for consitency - basevalues + variometervalues should return DI
        if debugmode:
            print ("Delta F", deltaF)

        # reformat time
        #linestruct.time = poslst[determinationindex].time
        # Putting data to linestruct:
        linestruct.x = inc
        linestruct.z = fstart
        linestruct.f = fstart #np.array([basex,basey,basez])
        linestruct.dx = basex
        if np.isnan(basex):
            linestruct.dy = np.nan
        if xyzorient:
            # in case of xyz use the first inclination measurement as reference. for hdz we follow the DTU approach
            linestruct.y = dec
            #linestruct.time = poslst[determinationindex].time
            linestruct.dy = basey
        linestruct.dz = basez
        linestruct.df = deltaF
        linestruct.typ = 'idff'
        linestruct.t2 = calcscaleval
        linestruct.var4 = s0i  # replaces Mirediff from _calcdec
        linestruct.var5 = epzi
        linestruct.str4 = str4 # Holds F type
        #print ("Basevalues", basex,linestruct.dy,basez)
        #if debugmode:
        #    print ("Fstart", fstart)

        return linestruct, hstart, h_adder
        #return linestruct, xstart, ystart

    def calcabsolutes(self, debugmode=None, **kwargs):
        """
        DEFINITION:
            Calculates DI values by calling calcdec and calcinc methods.
            Uses an iterative approach of dec and inc calculation, adopted from the DTU sheet.
            Differences:
                - Variometer correction and residual correction are performed at each individual step
                   and not average and rounded for repeated measurements before (should be better
                   and correct especially for residual correction)
                - Scale values are provided in deg/unit and not gon/unit
            Provide variometer and scalar values for optimal results.
            If no variometervalues are provided then only dec and inc are calculated correctly.
            Method:
                - DI Measurement is analyzed
                - Scalar data used in the following order: 1) data within file (assumed to be
                  obtained from the same pier - eventually add dF in Header 2) data from provided
                  scalar data set
                  (if available D, I, and F is given)
                - variation data

        PARAMETERS:
        variable:
            - line              (LineStruct) a line containing results from _calcdec()
        kwargs:
            - annualmeans:      (list of floats) a list providing Observatory specific annual mean values in x,y,z nT (e.g. [20000,1000,43000])
            - incstart          (float) - default 45.0 - inclination value in deg
            - deltaI:           (float) - default 0.0 - eventual correction factor for inclination in degree
            - usestep:          (int) use first, second or both of successive measurements (e.g. autodif requires usestep=2)
            - iterator:         (int) switch of loggerabs (e.g. vario not present) in case of iterative approach
            - scalevalue:       (list of floats) scalevalues for each component (e.g. default = [1,1,1])
            - debugmode:        (bool) activate additional debug output -- !!!!!!! removed and replaced by loggin !!!!!
            - printresults      (bool) - if True print results to screen
            - meantime          (bool) if true, values are recalculated to nearest
                                       horizontal measurement to average time

        USED BY:

        REQUIRES:

        RETURNS:

        EXAMPLE:
            stream.calcabsolutes(usestep=2,annualmeans=[20000,1200,43000],printresults=True)

        APPLICATION:
            absst = abs_read(path_or_url=abspath,azimuth=azimuth,output='DIListStruct')
            stream = elem.get_abs_distruct()
            variostr = read(variopath)
            variostr =variostr.rotation(alpha=varioorientation_alpha, beta=varioorientation_beta)
            vafunc = variostr.interpol(['x','y','z'])
            stream = stream._insert_function_values(vafunc)
            scalarstr = read(scalarpath)
            scfunc = scalarstr.interpol(['f'])
            stream = stream._insert_function_values(scfunc,funckeys=['f'],offset=deltaF)
            result = stream.calcabsolutes(usestep=2,annualmeans=[20000,1200,43000],printresults=True)
        """

        incstart = kwargs.get('incstart')
        scalevalue = kwargs.get('scalevalue')
        annualmeans = kwargs.get('annualmeans')
        unit = kwargs.get('unit')
        xstart = kwargs.get('xstart')
        ystart = kwargs.get('ystart')
        hstart = kwargs.get('hstart')
        hbasis = kwargs.get('hbasis')
        deltaD = kwargs.get('deltaD')
        deltaI = kwargs.get('deltaI')
        meantime = kwargs.get('meantime')
        usestep = kwargs.get('usestep')
        residualsign = kwargs.get('residualsign')
        printresults = kwargs.get('printresults')
        variometerorientation = kwargs.get('variometerorientation')

        if not deltaI:
            deltaI = 0.0
        if not deltaD:
            deltaD = 0.0
        if not scalevalue:
            scalevalue = [1.0,1.0,1.0]
        if not usestep:
            usestep = 0
        if not annualmeans:
            annualmeans = [0.0,0.0,0.0]
        if not xstart:
            xstart = annualmeans[0]
        if not ystart:
            ystart = annualmeans[1]
        if not hstart:
            hstart = np.sqrt(annualmeans[0]**2 + annualmeans[1]**2)
        if not incstart and not annualmeans == [0.0,0.0,0.0]:
            incstart = 180/np.pi * np.arctan(annualmeans[2] / np.sqrt(annualmeans[0]*annualmeans[0] + annualmeans[1]*annualmeans[1]))
        else:
            incstart = 0.0
        if not residualsign and not residualsign in [1,-1]:
            residualsign = 1

        ybasis = 0.0
        variocorrold = 0.0
        xyzo = False
        if variometerorientation in ["XYZ","xyz"]:
            if debugmode:
                print (" calcabsolutes: baseline output (and variometer input) in geographic system XYZ")
                print (" ------------------------------------------------------------------------------")
            xyzo = True

        for i in range(0,3):
            # Calculate declination value (use xstart and ystart as boundary conditions
            #print ("STarting with", xstart, ystart)
            #debugmode = True
            if debugmode:
                print ("Running cycle {}: ybasis = {}, hbasis={}".format(i, ybasis,hbasis))
            resultline, decmeanx, decmeany, variocorrold = self._calcdec(xstart=xstart,ystart=ystart,hstart=hstart,hbasis=hbasis,ybasis=ybasis,deltaD=deltaD,usestep=usestep,scalevalue=scalevalue,iterator=i,annualmeans=annualmeans,meantime=meantime,xyzorient=xyzo,residualsign=residualsign,debugmode=debugmode)
            # Calculate inclination value
            #print("Calculated D (%f) - iteration step %d" % (resultline[2],i))
            if debugmode:
                print("Calculated D (%f) - iteration step %d" % (resultline[2],i))
            # requires succesful declination determination
            if np.isnan(resultline.y):
                try:
                    loggerabs.error('%s : CalcAbsolutes: Declination could not be determined - aborting' % num2date(self[0].time).replace(tzinfo=None))
                except:
                    loggerabs.error('unkown : CalcAbsolutes: Declination could not be determined - aborting')
                break
            # use incstart and ystart as boundary conditions
            try: # check, whether outline already exists
                if np.isnan(outline.x):
                    inc = incstart
                else:
                    inc = outline.x
            except:
                inc = incstart
            outline, hstart, hbasis = self._calcinc(resultline,scalevalue=scalevalue,incstart=inc,deltaI=deltaI,iterator=i,usestep=usestep,annualmeans=annualmeans,xyzorient=xyzo,decmeanx=decmeanx,decmeany=decmeany,variocorrold=variocorrold,residualsign=residualsign,debugmode=debugmode)
            #outline, xstart, ystart = self._calcinc(resultline,scalevalue=scalevalue,incstart=inc,deltaI=deltaI,iterator=i,usestep=usestep,annualmeans=annualmeans)
            if xyzo:
                #print ("XYZ data: using ybasis")
                ybasis = outline.dy
            if debugmode:
                print("Calculated I (%f) - iteration step %d" %(outline[1],i))
                print("All results: " , outline)

        # Temporary cleanup for extraordinary high values (failed calculations) - replace by 999999.99
        for key in DataStream().FLAGKEYLIST:
             if not 'time' in key:
                 testval = eval('outline.'+key)
                 try:
                     testval = float(testval)
                     if testval > 10000000:
                         exec('outline.'+key+' = 999999.99')
                 except:
                     pass

        #test whether outline is a linestruct object - if not use an empty object
        try:
            test = outline.x

            loggerabs.info('%s : Declination: %s, Inclination: %s, H: %.1f, F: %.1f' % (num2date(outline.time), deg2degminsec(outline.y),deg2degminsec(outline.x),outline.f*np.cos(outline.x*np.pi/180),outline.f))

            if printresults:
                print('  Vector at: {}'.format(str(num2date(outline.time))))
                print('  Declination: %s, Inclination: %s, H: %.1f, Z: %.1f, F: %.1f' % (deg2degminsec(outline.y),deg2degminsec(outline.x),outline.f*np.cos(outline.x*np.pi/180),outline.f*np.sin(outline.x*np.pi/180),outline.f))
                print('  Collimation and Offset:')
                print('  Declination:    S0: %.3f, delta H: %.3f, epsilon Z: %.3f\n  Inclination:    S0: %.3f, epsilon Z: %.3f\n  Scalevalue: %.3f deg/unit' % (outline.var1,outline.var2,outline.var3,outline.var4,outline.var5,outline.t2))
                if outline.str4:
                    ty = str(outline.str4)
                else:
                    ty = ""
                outl = True
                try:
                    if np.isnan(outline.df):
                        outl = False
                except:
                    outl = False
                if outline.df and ty.startswith("Fabs") and outl:
                    print('deltaF determined ({}-Fext): {:.3f}'.format(outline.str4,outline.df))
                elif outline.df and ty.startswith("Fext") and outl:
                    print('deltaF provided ({}): {:.3f}'.format(outline.str4,outline.df))

        except:
            text = 'calcabsolutes: invalid LineStruct Object returned from calcinc function'
            loggerabs.warning(text)
            if printresults:
                print(text)
            outline = LineStruct()

        return outline

#
#  Now import the format libraries
#

from magpy.lib.magpy_absformats import *


def _analyse_di_source(didatasource, db=None, starttime=None, endtime=None, fileidentifier='.txt', debug=False):
    """
    DESCRIPTION
        analyse the source of di data, read it and then create dictionary with days (date) as key and
        an absolute data list (as returned by abs_read) as value.
    RETURNS
        dict : {dattime.date, {'absdata': abslist, 'source' : 'file', etc}
        failinglist : list , a list containing filenames of failed file readings
    VARIABLES
        didatasource : (string, list) pointing towards di data
        db : a database.DataBank() object
    APPLICATION:
        requires dilines_from_db,
    """
    source = None  # can be 'db', 'files', 'urls', 'webservice' ,'dilinestruc' or None
    resultsdict = {}
    tablename = ''  # will be filled with an eventually existing tablename for didata
    if starttime:
        starttime = testtime(starttime)
    else:
        starttime = datetime(1777,4,30)
    if endtime:
        endtime = testtime(endtime)
    else:
        endtime = datetime.now(timezone.utc).replace(tzinfo=None)

    if not didatasource:
        source = None
    elif isinstance(didatasource, basestring):
        # data base table or single file/url
        if db:
            # check if didatasource exists as table in table
            cursor = db.db.cursor()
            tablesql = "SHOW TABLES LIKE '{}'".format(didatasource)
            msg = db._executesql(cursor, tablesql)
            if msg:
                print(" _analyse_di_source: accessing data base tables failed: {}".format(msg))
            else:
                tablenamel = cursor.fetchone()
                if tablenamel and len(tablenamel) > 0:
                    tablename = tablenamel[0]
                    source = 'db'
                    # Table is existing
        if not source:
            # if string is not corresponding to a database table
            # string is an url or a filename
            didatasource = [didatasource]
    filelist = []
    if isinstance(didatasource, (list, tuple)):
        # multiple files/urls or an abslist
        for elem in didatasource:
            if str(type(elem).__name__) == "DILineStruct":
                source = 'dilinestruct'
            elif "://" in elem:
                source = 'urls'
                if debug:
                    print(" _analyse_di_source:  Found URL code - requires name of data set with date")
                if "observation.json" in elem:
                    source = 'webservice'
            elif os.path.isfile(elem):
                source = 'files'
                filelist.append(elem)
            elif os.path.exists(elem):
                # directory
                source = 'files'
                for file in os.listdir(str(elem)):
                    if debug:
                        print("  _analyse_di_source:  scanning for {} (do not include wildcards)".format(fileidentifier))
                    if file.endswith(fileidentifier):
                        filelist.append(os.path.join(elem, file))
            else:
                print("  _analyse_di_source: can not interpret the following elem of absdata:", elem)

    dilines = []
    acceptedfiles = []
    failedfiles = []
    if debug:
        print("  _analyse_di_source: Identified data source:", source)
    if not source:
        print("  _analyse_di_source:  did not fine a suitable source")
        return resultsdict
    elif source == 'db':
        # tablename is identified
        # get diline if starttime and endtime fit
        dilines = db.diline_from_db(starttime=starttime, endtime=endtime, tablename='DIDATA')
        # datelist = list(set(lst))
    elif source == 'dilinestruct':
        dilines = didatasource
    elif source in ['files', 'urls', 'webservice']:
        dilines = []
        acceptedfiles = []
        for fi in filelist:
            msg = "  _analyse_di_source: checking {} ...".format(fi)
            absst = abs_read(fi)  # azimuth, pier in old code - sort later
            if not absst:
                failedfiles.append(fi)
            for a in absst:
                dilines.append(a)
                acceptedfiles.append(fi)
                if not msg.endswith('SUCCESS'):
                    msg += " SUCCESS"
            if debug:
                print(msg)
    if debug:
        print("  _analyse_di_source: got {} DI data lines".format(len(dilines)))

    for idx,line in enumerate(dilines):
        mintime = datetime(1700,1,1)
        maxtime = datetime(1700,2,1)
        try:
            mintime = testtime(np.nanmin(line.time))
            maxtime = testtime(np.nanmax(line.time))
        except:
            pass
        if mintime >= starttime and maxtime <= endtime:
            mindate = mintime.date()
            maxdate = maxtime.date()
            contdict = resultsdict.get(mindate, {})
            dlines = contdict.get("dilines", [])
            fl = contdict.get("filelist", [])
            if mindate == maxdate:
                dlines.append(line)
                if acceptedfiles and len(acceptedfiles) >= idx and not acceptedfiles[idx] in fl:
                    fl.append(acceptedfiles[idx])
                contdict["dilines"] = dlines
                contdict["source"] = source
                contdict["filelist"] = fl
            else:
                print("  _analyse_di_source: DI measurement performed during two days - not yet supported - skipping")
            resultsdict[mindate] = contdict

    return resultsdict, failedfiles


def _logfile_len(fname, logfilter):
    f = open(fname,"rb")
    cnt = 0
    for line in f:
        if logfilter in line:
            cnt = cnt +1
    return cnt


def data_for_di(source, starttime, endtime=None, datatype='scalar', alpha=None, beta=None, magrotation=False,
                 compensation=False, offset=None, skipdb=False, db=None, flagfile=None, debug=False):
    """
    DESCRIPTION
        Analyzing source: Source is provided as a dictionary, multiple sources are allowed - if it is string or list (old type)
        accepted are file for filepath/url or database with a tuple
        if database and file is provided then firstly the database is accessed and if failing (no data) then the file is access
        #{'file': pathname , 'database': ( db, tablename ) }
    VARIABLES:
        db  (databank):        if db is provided then data.header information is updated from the selected database.
                               Compensation, delta and rotation values from the header are used for optional corrections.
                               Providing db will also trigger the application of flags from the database. Currently,
                               this is te only way to apply flags with the data_for_di method
        magrotation  (bool):   if True then data.header values for alpha and beta are used (i.e. 'DataRotationAlpha')
        alpha, beta  (floats): if magrotation=False and alpha and/or beta are provided they will be used for rotation
        compensation (bool):   if True then data.header values for bias field are used (i.e. 'DataCompensationX')
                               Bias fields in DB are given in !! microT !!.
                               Take of the sign: F_without_bias = F with_bias + DataCompensation
                               which is different from offset and delta F
        offset     (dict):     if provided then data.header data to apply_deltas and compensation corrections are
                               ignored. The provided offset values are used instead for correction.
        datatype  (string) :   either 'scalar' or 'vario' or 'both'
        skipdb (bool) :        data.header is not updated from the databank, even if db is provided. Will then also
                               ignore flagging and delta values from database even if this is the data source
        Return:
        # Move this part out of the method
        variometerorientation - is eventually modified by this code, but might also not be necessary - just makes sure that HEZ baselines
        are returned in case of non-xyz data
    RETURNS
        data (DataStream) with corrections applied and a DataFunctionObject containing an interpolation function
    APPLICATION
        # Data in files (or file like objects)
        data = data_for_di(example5, starttime="2018-08-29", datatype='both')
        # Webservice  data
        data = data_for_di("https://cobs.zamg.ac.at/gsa/webservice/query.php?id=WIC", starttime='2024-08-01',endtime='2024-08-02', datatype='both', debug=True)
        # Database data
        db = database.DataBank("localhost","maxmustermann","geheim","testdb")
        tablename = "TEST_0001_0001_0001"
        data = data_for_di({'db': (db,tablename)}, starttime='2022-11-22', endtime='2022-11-23', type='scalar')
    """
    from magpy.stream import DataStream, read
    #from magpy.core import flagging
    #from magpy.core import database

    if debug:
        print("-----------------")
        print(" Load data stream from {}".format(source))

    starttime = testtime(starttime)
    if not endtime:
        endtime = starttime + timedelta(days=1)

    data = DataStream()
    func = []
    datagood = True

    if not source:
        datagood = False
    elif isinstance(source, dict):
        tup = source.get('db', [])
        fi = source.get('file', None)
        if tup and len(tup) == 2:
            db = tup[0]
            data = tup[0].read(tup[1], starttime=starttime, endtime=endtime)
        if fi and not len(data) > 0:
            try:
                data = read(fi, starttime=starttime, endtime=endtime)
            except:
                data = DataStream()
        if len(data) > 0:
            if debug:
                print("   Successfully loaded data with version 2.0")
        else:
            datagood = False
    else:
        source = source.split(',')
        if len(source) > 2 and not len(source) > 0:
            datafailed = True
        elif len(source) == 2:  # old db type
            db = source[0]
            try:
                data = db.read(source[1], starttime=starttime, endtime=endtime)
            except:
                data = DataStream()
        else:
            try:
                data = read(source[0], starttime=starttime, endtime=endtime)
            except:
                data = DataStream()
        if len(data) > 0:
            if debug:
                print("   Successfully loaded data with version 1.0")
        else:
            datagood = False
    sensorid = data.header.get('SensorID')

    if datagood:
        if debug:
            print(" -> Obtained {} data points from {} of datatype {}".format(len(data), source, datatype))
        # check if a sensorid  is present - except ?
        # check if data is available in the required columns
        if datatype in ['scalar', 'both', 'full'] and not len(data.ndarray[data.KEYLIST.index('f')]) > 0:
            # Calculate F values if not existing. Please note: this method will consider eventually available delta F data
            data = data.calc_f()
        elif datatype in ['scalar']:
            # just continue - nothing to do yet
            pass
        elif datatype in ['vario', 'variometer', 'both', 'full']:
            variocomps = data.header.get('DataComponents', '').lower()
            if debug:
                print("  - variometer data contains the following components: {}".format(variocomps))
            if variocomps.startswith("hdz"):
                if debug:
                    print("  - variationdata as HDZ -> converting to XYZ")
                data = data._convertstream('hdz2xyz')
            elif variocomps.startswith("idf"):
                if debug:
                    print("  - variationdata as IDF -> converting to XYZ")
                data = data._convertstream('idf2xyz')
            elif variocomps.startswith("hez"):
                if debug:
                    print("  - variationdata as HEZ -> fine")
            else:
                if not variocomps.startswith("xyz"):
                    print("  - variationdata has an orientation which MagPy cannot handle")
                    print("  - continuing assuming HEZ")
        else:
            print('  - unknown datatype')
            datagood = False

        sensorid = data.header.get('SensorID', '')
        if not sensorid:
            print(" Be careful: No Sensor ID available for {}".format(datatype))

        if sensorid and datagood and flagfile and os.path.isfile(flagfile):
            fl = flagging.load(flagfile, sensorid, starttime, endtime)
            if len(fl) > 0:
                data = fl.apply_flags(data, mode='drop')

        if db and sensorid and not skipdb and datagood:
            # Drop flagged data
            fl = db.flags_from_db(sensorid, starttime=starttime, endtime=endtime)
            if len(fl) > 0:
                data = fl.apply_flags(data, mode='drop')
            if debug:
                print("  -> applied {} flags from data base ...".format(len(fl)))
            # get all header data from database and apply delta values (i.e. F offsets etc)
            if data.header.get('DataID',''):
                dataid = data.header.get('DataID','')
            else:
                dataid = data.header.get('SensorID') + '_0001'
            data.header = db.fields_to_dict(dataid)
            if debug:
                print("  -> applied header from data base ...")
        if not offset and sensorid and datagood:  # TODO check that - not done in MagPy 1.x
            data = data.apply_deltas(debug=debug)
            if debug:
                print("  -> applied delta_values contained in data stream, evenually previously extracted from data base ...")
                # print (" ------------  IMPORTANT ----------------")
                # print (" Both, deltaF from DB and the provided delta F {b}".format(b=deltaF))
                # print (" will be applied.")

        if not len(data) > 0:  # still
            datagood = False

    if datagood and datatype in ['vario', 'variometer', 'both', 'full']:
        if magrotation or compensation and not data.header.get('DataDeltaValuesApplied', False) and not offset:
            offdict = {}
            xcomp = data.header.get('DataCompensationX', '0')
            ycomp = data.header.get('DataCompensationY', '0')
            zcomp = data.header.get('DataCompensationZ', '0')
            if not float(xcomp) == 0.:
                offdict['x'] = -1 * float(xcomp) * 1000.
            if not float(ycomp) == 0.:
                offdict['y'] = -1 * float(ycomp) * 1000.
            if not float(zcomp) == 0.:
                offdict['z'] = -1 * float(zcomp) * 1000.
            data = data.offset(offdict)
            print('  -> applied compensation fields: x={}, y={}, z={}'.format(xcomp, ycomp, zcomp))
        elif offset:
            data = data.offset(offset)

        rotated = False
        if magrotation:
            valalpha = 0.0
            valbeta = 0.0
            if not is_number(alpha):
                # db header has already been applied and alpha is not provided
                rotstring = data.header.get('DataRotationAlpha', '')
                rotdict = string2dict(rotstring, typ='oldlist')
                # print ("Dealing with year", date.year)
                valalpha = rotdict.get(str(starttime.year), '')
                if valalpha == '':
                    print("     no alpha value found for year {}".format(starttime.year))
                    try:
                        maxkey = max([int(k) for k in rotdict])
                        valalpha = rotdict.get(str(maxkey), 0)
                        print("  -> using alpha for year {}".format(str(maxkey)))
                    except:
                        valalpha = 0
                valalpha = float(valalpha)
                if not float(valalpha) == 0.:
                    print("  -> rotating with alpha: {a} degree (year {b})".format(a=valalpha, b=starttime.year))
            else:
                # Using manually provided rotation value - see below
                pass
            if not is_number(beta):
                rotstring = data.header.get('DataRotationBeta', '')
                rotdict = string2dict(rotstring, typ='oldlist')
                valbeta = rotdict.get(str(starttime.year), '')
                if valbeta == '':
                    try:
                        maxkey = max([int(k) for k in rotdict])
                        valbeta = rotdict[str(maxkey)]
                    except:
                        valbeta = 0
                valbeta = float(valbeta)
                if not float(valbeta) == 0.:
                    print("  -> rotating with beta: {a} degree (year {b})".format(a=valbeta, b=starttime.year))
            else:
                # Using manually provided rotation value - see below
                pass
            if valalpha != 0 or valbeta != 0:
                data = data.rotation(alpha=valalpha, beta=valbeta)
                data.header['DataComments'] = "{} - rotated by alpha={} and beta={}".format(data.header.get('DataComments', ''), valalpha, valbeta)
                rotated = True
        if is_number(alpha) or is_number(beta) and not rotated:  # if alpha and beta are provided then rotate anyway if not done yet
            if not alpha == 0.0 or not beta == 0.0:
                if debug:
                    print (" alpha and/or beta provided - rotating with these manual values: alpha={} and beta={}".format(alpha, beta))
                if is_number(alpha):
                    valalpha = alpha
                else:
                    valalpha = 0.0
                if is_number(beta):
                    valbeta = beta
                else:
                    valbeta = 0.0
                if valalpha != 0 or valbeta != 0:
                    data = data.rotation(alpha=valalpha, beta=valbeta)
                    data.header['DataComments'] = "{} - rotated by alpha={} and beta={}".format(
                    data.header.get('DataComments', ''), valalpha, valbeta)
                    if debug:
                        print("  -> rotating with manually provided alpha {} and beta {}".format(valalpha, valbeta))
        if not len(data) > 0:  # still
            datagood = False

    if datagood:
        if (len(data) > 3 and not np.isnan(data.mean('time'))) or len(
                data.ndarray[0]) > 0:  # Because scalarstr can contain ([], 'File not specified')
            if datatype == 'scalar':
                func = data.interpol(['f'])
            elif datatype in ['vario', 'variometer']:
                func = data.interpol(['x', 'y', 'z'])
            elif datatype in ['both', 'full']:
                func = data.interpol(['x', 'y', 'z', 'f'])
            if debug:
                print(
                    "  -> interpolation function determined - data at DI timesteps will be obtained from interpolated data ...")
            if func[0] == {}:
                print(
                    "  !! function determination apparently failed: {} data of {} seems to be invalid".format(datatype,
                                                                                                              sensorid))
                datagood = False
            else:
                data.header['DataFunctionObject'] = [func]

    if datagood:
        if debug:
            print(" data set {}: projected correction methods applied".format(sensorid))
            print("-----------------")
        return data
    else:
        if debug:
            print(" data set {}: projected correction methods failed".format(sensorid))
            print("-----------------")
        return DataStream()


def deg2degminsec(value):
    """
    Function to convert deg.decimals to a string with deg min and seconds.decimals
    """
    if value > 360:
        return 'larger than 360'

    if isnan(value):
        return 'nan'

    degree = int(value)
    tminutes = np.abs(value - int(value))*60
    minutes = int(tminutes)
    tseconds = (tminutes-minutes)*60
    seconds = int(np.round(tseconds))
    if seconds == 60:
        minutes = minutes+1
        seconds = 0
    if minutes == 60:
        if degree < 0:
            degree = degree - 1
        else:
            degree = degree + 1
        minutes = 0

    return str(degree)+":"+str(minutes)+":"+str(seconds)


@deprecated("absRead renamed to abs_read")
def absRead(path_or_url=None, dataformat=None, headonly=False, **kwargs):
    return abs_read(path_or_url=None, dataformat=None, headonly=False, **kwargs)


def abs_read(path_or_url=None, dataformat=None, headonly=False, output='DIListStruct', debug=False, **kwargs):
    """
    DESCRIPTION
        Read DI from file or url for analysis
        Data is stored in an AbsoluteData() container
    VARIABLES
        dataformat (string) : select the input format,  if none then autodetect
        headonly (bool) : not supported so far
        output(string) : select the output format. Default is 'DIListStruct' which will return a list containing
                         individual DIListStuct's
                         An alternative output format is the old 'AbsoluteDIStruct'
    """

    if not output:
        output = 'DIListStruct'
    # -- No path
    if not path_or_url:
        messagecont = "File not specified"
        return [],messagecont
    # -- Create Data Container
    #stream = AbsoluteData()
    stream = AbsoluteData()
    # -- Check file
    if not isinstance(path_or_url, basestring):
        # not a string - we assume a file-like object
        pass
    elif "://" in path_or_url:
        # some URL
        # extract extension if any
        suffix = '.'+os.path.basename(path_or_url).partition('.')[2] or '.tmp'
        name = os.path.basename(path_or_url).partition('.')[0] # append the full filename to the temporary file
        fh = NamedTemporaryFile(suffix=name+suffix,delete=False)
        fh.write(urlopen(path_or_url).read())
        fh.close()
        stream = _abs_read(fh.name, dataformat, headonly, output=output, debug=debug, **kwargs)
        os.remove(fh.name)
    else:
        # some file name
        pathname = path_or_url
        stream = _abs_read(pathname, dataformat, headonly, **kwargs)
        if len(stream) == 0:
            # try to give more specific information why the stream is empty
            if has_magic(pathname) and not glob(pathname):
                raise Exception("No file matching file pattern: %s" % pathname)
            elif not has_magic(pathname) and not os.path.isfile(pathname):
                raise IOError(2, "No such file or directory", pathname)
    return stream


def _abs_read(filename, dataformat=None, headonly=False, output='DIListStruct', debug=False, **kwargs):
    """
    DESCRIPTION
        Reads a single file and returns an AbsoluteData structure
    VARIABLES
        dataformat (string) : select the input format,  if none then autodetect
        headonly (bool) : not supported so far
        output(string) : select the output format. Default is 'DIListStruct' which will return a list containing
                         individual DIListStuct's
                         An alternative output format is the old 'AbsoluteDIStruct'
    """

    # get format type
    format_type = None
    if not dataformat:
        # auto detect format - go through all known formats in given sort order
        for format_type in AbsoluteData().MAGPY_SUPPORTED_ABSOLUTES_FORMATS:
            # check format
            if isAbsFormat(filename, format_type):
                break
    else:
        # format given via argument
        dataformat = dataformat.upper()
        try:
            formats = [el for el in AbsoluteData().MAGPY_SUPPORTED_ABSOLUTES_FORMATS if el == dataformat]
            format_type = formats[0]
        except IndexError:
            msg = "Format \"%s\" is not supported. Supported types: %s"
            raise TypeError(msg % (dataformat, ', '.join(AbsoluteData().MAGPY_SUPPORTED_ABSOLUTES_FORMATS)))
    # file format should be known by now
    if debug:
        print("DI format:", format_type)

    stream = readAbsFormat(filename, format_type, headonly=headonly, output=output, **kwargs)

    # TODO If stream could not be read return an empty datastream object
    try:  # check this and replace by something smart
        xxx = len(stream)
    except:
        stream = AbsoluteData()

    return stream


@deprecated("please use absolute_analysis in the future")
def absoluteAnalysis(absdata, variodata, scalardata, **kwargs):
    return absolute_analysis(absdata, variodata, scalardata, **kwargs)


def absolute_analysis(absdata, variodata, scalardata, **kwargs):
    """
    DEFINITION:
        Single method access to analyze absolute data from any selected source. This method will create
        a datastream with results.

    PARAMETERS:
      Variables:
        - header_dict:  (dict) dictionary with header information
        variodata:      (string) path to variodata, can include wildcards like /my/path/*.sec
        scalardata:     (string) path to scalardata, can include wildcards like /my/path/*.sec
      Optional (often used):
        - starttime:    (string/datetime) define begin
        - endtime:      (string/datetime) define end
        - db:           (mysql database) defined by magpy.core.database.databank()
        - deltaF:       (float) difference between scalar measurement point and pier (F(DIflux-pier) - F(scalar-pier))??
        - deltaD:       (float) = kwargs.get('deltaD')
        - deltaI:       (float) = kwargs.get('deltaI')
        - diid:         (string) identifier (id) of di files (e.g. A2_WIC.txt, don't use wildcards like *)
        - pier:         (string) only analyse data from this pier i.e. A2 will only select pier A2 data from source
        - azimuth:      (float) if not contained in absdata - i.e. required for Autodif raw measurements
        - variometerorientation: (string) accepts XYZ, xyz, in any other case HEZ is asumed
        - expD:         (float) expected Declination - failure produced when D differs by more than expT deg
        - expI:         (float) expected Inclination - failure produced when I differs by more than expT deg
        - expT:         (float) expected value threshold - default 2 deg
        - stationid:    (string) provide the stationid if you use archiving or dbadd
        - dbadd:        (string) if provided then DI-raw data will be added to the given database table i.e. dbadd='DIDATA'
                                 Caution: this method is using the "replace" mode
        - movetoarchive:(string) define a local directory to store archived data. The directory needs to exist already.
        - alpha:        (float) orientation angle in declination plane (deg)
        - beta:         (float) orientation angle in inclination plane (deg)
        - magrotation   (bool) rotate variometer data using rotationvalues in database, requires db.
                               given alpha and beta values by above options will override db data.
                               if magrotation is selected, compensation=True will always be applied.
        - compensation  (bool) apply fluxgate compensation values (i.e. LEMI) to variometer data
                            so that the obtained vector is representing some reasonable geomagnetic field
                            vector. compensation values will be taken from header or db. By provinding
                            option 'offset' you can override these values by a user choice
        - offset        (dict)
        - residualsign: (int) either +1 (default) or -1. defines the orientation of the fluxgate probe on
                              the theodolite. This value is important when using the residual method and might
                              be different for different theodolites (please consider for data selection)

      Optional (seldom used):
        - abstype:      (string) default 'manual', other options: 'autodif' (is actually automatically selected)
        - outputformat: (string) one of 'idf', 'xyz', 'hdf' (default is idf)
        - usestep:      (int) which step to use for analysis, usually both, in autodif raw = 2
        - annualmeans:  (list) provide annualmeans X, Y, Z as a list of these components in nT
                               i.e. [21124.45, 1723.12, 44679.97]. Providing annualmeans makes sense
                               if you don't have any F measurements
        - meantime:     (bool) default=False. Results will be averaged on the time of the first measurement
                               if meantime=True results are calculated at the horizontal measurement nearest
                               to average time
        - skipvariodb:  (bool) default False. If True then, even if db is provided, then NEITHER the
                               data header is updated from the database, NOR flags and 'apply_deltas'
                               from the database is performed for variometer data
        - skipscalardb:  (bool) default False. If True then, even if db is provided, then NEITHER the
                               data header is updated from the database, NOR flags and 'apply_deltas'
                               from the database is performed for scalar data
        - flagfile:     (path) default None. Provide flagging information for variometer and scalar data


    RETURNS:
        DataStream()    a datastream containing absolute directions, basevalues for the input instruments and
                        collimation angles as well as information on the measurement

    EXAMPLE:
        # a single absolute file and variable source for variometer data
        basevalues = absolute_analysis(example6a, {'file':example5, 'db':(db,'WIC_1_0001_0001')}, example5, db=db, starttime="2018-08-28", endtime="2018-08-30")
        # multiple absolute files and adding results to database tabe DIDATA
        basevalues = absolute_analysis([example6a,example6b], {'file':example5, 'db':(db,'WIC_1_0001_0001')}, example5, db=db, starttime="2018-08-28", endtime="2018-08-30", dbadd='DIDATA')
        # read DI data from database and save raw DI data to archive (will be moved in cas of DI files instead of DI database)
        basevalues = absolute_analysis('DIDATA', {'file':example5, 'db':(db,'WIC_1_0001_0001')}, example5, db=db, starttime="2018-08-28", endtime="2018-08-30", movetoarchive='/tmp')
        # read DI data from directory with diid identifier
        basevalues = absolute_analysis('/home/leon/Dropbox/Daten/Magnetism/DI-WIC/raw/', variopath, scalarpath, diid='A2_WIC.txt', starttime='2014-02-10',endtime='2014-02-25',db=db,dbadd=True)

        (1) stream = absolute_analysis('/home/leon/Dropbox/Daten/Magnetism/DI-WIC/autodif',variopath,'',abstype='autodif',azimuth=267.4242,starttime='2014-02-10',endtime='2014-02-25')

    PERFORMED TESTS:
        (okOK)1. Read data from database and files
        (OK)2. Read data from autodif and manual analysis
        (okOK)3. Put data to database (dbadd)
        (okOK)4. Check parameter for all possibilities: a) no db, b) db, c) db and override by input
        (okOK)5. Archiving function
        (okOK)6. Appropriate information on failed analyses -> add test for expected values
        (OK)7. is the usestep variable correctly applied for autodif and normal?
        (OK)8. overwrite of existing database lines?
        (OK)9. Order of saving data when analyzing older data sets - requires reload and delete
        10. Test memory capabilities for large data sets (~ 6month with two varios, one scalar, no vario and scalar data)
        (OK)11. Check URL/FTP read, single file and directory read

        Bugs:
        when importing db2diline from database, the DILineStruct, defined in absolutes is missing.... why???????? see Test4 in FullAbsoluteAnalysis -- order of import is very important

    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    db = kwargs.get('db')
    alpha = kwargs.get('alpha')
    beta = kwargs.get('beta')
    deltaF = kwargs.get('deltaF')
    deltaD = kwargs.get('deltaD')
    deltaI = kwargs.get('deltaI')
    diid = kwargs.get('diid')
    outputformat = kwargs.get('outputformat')
    usestep = kwargs.get('usestep')
    annualmeans = kwargs.get('annualmeans')
    variometerorientation = kwargs.get('variometerorientation')
    azimuth = kwargs.get('azimuth')  # 267.4242 # A16 to refelctor
    expT = kwargs.get('expT')
    expI = kwargs.get('expI')
    expD = kwargs.get('expD')
    dbadd = kwargs.get('dbadd')
    movetoarchive = kwargs.get('movetoarchive')
    abstype = kwargs.get('abstype')
    pier = kwargs.get('pier')
    stationid = kwargs.get('stationid')
    magrotation = kwargs.get('magrotation')
    compensation = kwargs.get('compensation')
    meantime = kwargs.get('meantime')
    residualsign = kwargs.get('residualsign')
    offset = kwargs.get('offset')
    skipvariodb = kwargs.get('skipvariodb')
    skipscalardb = kwargs.get('skipscalardb')
    skipvariodb = kwargs.get('skipvariodb')
    skipscalardb = kwargs.get('skipscalardb')
    flagfile = kwargs.get('flagfile')

    scalevalue = kwargs.get('scalevalue')
    debug = kwargs.get('debug')

    # residualsign=-1
    # debug=True
    if not outputformat:
        outputformat = 'idf'
    if not annualmeans:
        # annualmeans=[20000,1200,43000]
        annualmeans = [0.0, 0.0, 0.0]
    if not variometerorientation:
        variometerorientation = "HEZ"
    if not abstype:
        abstype = "manual"
    if abstype == 'autodif':  # we also check the person given in the file. if this is AutoDIF abstype will be set correctly automatically
        usestep = 2
        if not azimuth:
            print("Azimuth needs ro be provided for AutoDIF measurements")
            return
    if not expT:
        expT = 2
    if not deltaF:
        deltaF = 0.0
    if not stationid:
        stationid = ''
    if not pier:
        pier = ''
    if not diid:
        diid = ".txt"
    if not residualsign:
        residualsign = 1
    if starttime:
        starttime = testtime(starttime)
    if endtime:
        endtime = testtime(endtime)

    varioid = 'Unknown'
    scalarid = 'Unknown'
    # ####################################
    # 2. Get absolute data
    # ####################################

    # 2.1 Database or File -> Get a datelist
    # --------------------
    filelist, datelist = [], []
    failinglist = []
    failingdict = {}  # contains {failedsource : reason, ...}
    successlist = []
    successfiles = []
    difiles = []
    datastructok = True
    KEYLIST = DataStream().KEYLIST
    NUMKEYLIST = DataStream().NUMKEYLIST

    resultdict, failinglist = _analyse_di_source(absdata, db=db, starttime=starttime, endtime=endtime,
                                                    fileidentifier=diid, debug=debug)
    if debug:
        print ("Data sets returned from _analyse:di:source", resultdict)
    for fa in failinglist:
        failingdict[fa] = "analysing di source failed"
    # 2.2 Cycle through dates
    # --------------------
    # read varios, scalar and all absfiles of one day
    # analyze data for each day and append results to a resultstream
    # XXX possible issues: an absolute measurement which is performed in two day (e.g. across midnight)
    if expI or expD:
        print("")
        print("Thresholds for valid data: Declination within {a}+/-{c} deg, Inclination within {b}+/-{c} deg".format(
            a=expD, b=expI, c=expT))
        print("")

    resultstream = DataStream()
    for date in resultdict:
        variofound = True
        scalarfound = True
        print("")
        print("------------------------------------------------------")
        print("Starting analysis for ", date)
        print("------------------------------------------------------")
        st = datetime.combine(date, datetime.min.time())
        et = datetime.combine(date, datetime.max.time())
        # a) Read variodata
        datatype = 'vario'
        msg = " Variation data analysis ..."
        if variodata == scalardata:
            datatype = 'both'
            print("variodata equals scalardata")
        vdata = data_for_di(variodata, starttime=st, endtime=et, datatype=datatype, alpha=alpha, beta=beta,
                            magrotation=magrotation, flagfile=flagfile, db=db,
                            compensation=compensation, offset=offset, skipdb=skipvariodb, debug=debug)
        if len(vdata) > 0:
            print("{} OK".format(msg))
        else:
            print("{} no data".format(msg))
        # b) Load Scalardata
        msg = " Scalar data analysis ..."
        if not datatype == 'both':
            sdata = data_for_di(scalardata, starttime=st, endtime=et, datatype='scalar', alpha=alpha, beta=beta,
                                magrotation=magrotation, flagfile=flagfile, db=db,
                                compensation=compensation, offset=offset, skipdb=skipscalardb, debug=debug)
        else:
            sdata = vdata.copy()
        if len(sdata) > 0:
            print("{} OK".format(msg))
        else:
            print("{} no data".format(msg))
        # c) get absolute data
        contdict = resultdict.get(date)
        files = contdict.get('filelist')
        abslist = contdict.get('dilines')

        for ab in abslist:
            print("-----------------")
            # get pier
            try:
                distruct = ab.get_abs_distruct()
                datastructok = True
            except:
                datastructok = False
                if not failingdict.get(st, None):
                    failingdict[st] = "problem with data structure"
            if pier and not pier == ab.pier:
                print("Differences between projected pier ({}) and pier in data struct ({}) - skipping".format(pier,
                                                                                                               ab.pier))
                if not failingdict.get(st, None):
                    failingdict[st] = "piers provided in data file and analysis command do not fit"
            elif datastructok:
                if azimuth:
                    for i in range(len(distruct.container)):
                        distruct[0].expectedmire = azimuth
                if ab.person == 'AutoDIF':
                    abstype = 'autodif'
                    if not usestep:
                        usestep = 2
                    if not azimuth:
                        print("absolute_analysis: AUTODIF but no azimuth provided --- this will not work")

                print(" Analyzing {} measurement from {} with given azimuth {}".format(abstype, date,
                                                                                       distruct[0].expectedmire))

                if len(vdata) > 0:
                    valuetest = distruct._check_coverage(vdata)
                    if valuetest:
                        func = vdata.header.get('DataFunctionObject')[0]
                        distruct = distruct._insert_function_values(func, funckeys=['x', 'y', 'z'], debug=debug)
                    else:
                        print(" Warning! Variation data missing at DI time range")
                    # Check orinetation
                    variocomps = vdata.header.get('DataComponents','').lower()
                    if variocomps.startswith("xyz") and not variometerorientation.lower() == "xyz":
                        print("  Variometer data provided in XYZ, Basevalue output projected in HDZ, however,")
                        print("  as variometerorientation is not manually confirmed to be xyz (see manual) ")
                    elif not variocomps.startswith("xyz") and variometerorientation.lower() == "xyz":
                        print("  Basevalue output projected in XYZ but variometer data provided in HEZ!")
                        print("  MagPy does not yet support that yet - switching to HDZ basevalues")
                        variometerorientation = "HEZ"
                if len(sdata) > 0:
                    valuetest = distruct._check_coverage(sdata, keys=['f'])
                    if valuetest:
                        func = sdata.header.get('DataFunctionObject')[0]
                        distruct = distruct._insert_function_values(func, funckeys=['f'], offset=deltaF, debug=debug)
                    else:
                        print(" Warning! Scalar data missing at DI time range")
                # get delta D and delta I values here
                if not deltaD and db:
                    deltaD = db.get_pier(pier, 'A2', value='deltaD', year=st.year)
                if not deltaI and db:
                    deltaI = db.get_pier(pier, 'A2', value='deltaI', year=st.year)
                # if not deltaF and db:  # check that - not contained in MagPy 1.x
                #    deltaF = db.get_pier(pier, 'A2', value='deltaF', year=starttime.year)

                if debug:
                    print("Provided pier differences (either by option (primary) or from database):")
                    print(" delta F for continuous scalar data: {}".format(deltaF))
                    print(" pier delta D: {}, pier delta I: {}".format(deltaD, deltaI))

                result = distruct.calcabsolutes(usestep=usestep, annualmeans=annualmeans, printresults=True,
                                                debugmode=debug, deltaD=deltaD, deltaI=deltaI, meantime=meantime,
                                                scalevalue=scalevalue, variometerorientation=variometerorientation,
                                                residualsign=residualsign)

                paralist = []
                if not deltaF == 0:
                    paralist.append("dF_{}".format(deltaF))
                comments = vdata.header.get('DataComments', '')
                if comments.find('alpha') > 0 or alpha:
                    if alpha:
                        paralist.append("alpha_{}".format(alpha))
                    else:
                        val = comments.split('alpha=')[1]
                        print(val)
                        calpha = val.split(" - ")[0]
                        paralist.append("alpha_{}".format(calpha))
                if comments.find('beta') > 0 or beta:
                    if beta:
                        paralist.append("beta_{}".format(beta))
                    else:
                        val = comments.split('beta=')[1]
                        print(val)
                        cbeta = val.split(" - ")[0]
                        paralist.append("beta_{}".format(cbeta))
                if len(paralist) > 0:
                    parastr = ",".join(paralist)
                    result.str4 = "{},{}".format(result.str4, parastr)
                    # if debug:
                    print("Parameterlist:", result.str4)
                dataok = True
                if expD:
                    if not float(expD) - float(expT) < result.y < float(expD) + float(expT):
                        print(" absolute_analysis: Failed to analyse {} - threshold for dec exceeded".format(
                            num2date(result.time)))
                        dataok = False
                if expI and dataok:
                    if not float(expI) - float(expT) < result.x < float(expI) + float(expT):
                        print(" absolute_analysis: Failed to analyse {} - threshold for inc exceeded".format(
                            num2date(result.time)))
                        dataok = False
                if dataok:
                    successlist.append(ab)
                    successfiles.append(files)
                    resultstream.add(result)
                else:
                    failinglist.append(files)
                    if not failingdict.get(st, None):
                        failingdict[st] = "provided validity threshold not met"

    if debug:
        print(" absolute_analysis: obtained  {} successful analyses".format(len(successlist)))

    # ####################################
    # 3. Archiving - adding to database
    # ####################################
    if len(successlist) > 0 and movetoarchive and os.path.isdir(movetoarchive):
        # save this particular data set to archive
        successfiles = np.asarray(successfiles).flatten()
        if len(successfiles) > 0:
            successfiles = list(set(successfiles))
        if debug:
            print(" absolute_analysis: adding DI raw data to archive {} ...".format(movetoarchive))
        if contdict.get('source') == 'files':
            # then copy the files paths to archive and overwrite existing data
            for fi in successfiles:
                source = fi
                destination = os.path.join(movetoarchive, os.path.split(fi)[1])
                if not source == destination:
                    # delete destination
                    if os.path.exists(destination):
                        if debug:
                            print("archive file already existing - replacing it")
                        os.remove(destination)
                    shutil.move(source, destination)
        else:
            for dat in successlist:
                dat.save_di(movetoarchive)
    if len(successlist) > 0 and dbadd and db:
        # add this particular data set into a database table
        if debug:
            print(" absolute_analysis: adding DI raw data to database ...")
        if not stationid:
            print(" absolute_analysis: error when adding data to db - please provide a stationID")
        else:
            tablename = 'DIDATA'
            if isinstance(dbadd, basestring):
                tablename = dbadd
            db.diline_to_db(successlist, mode='replace', tablename=tablename, stationid=stationid)
        if debug:
            print("                   ... done")

            # ####################################
    # 4. Format output - datastream
    # ####################################

    # 3.0 Convert result to ndarray and dx,dy,dz to XYZ in nT
    #     --- This is important for baseline correction as all variometer provided components in nT

    # cleanup resultsstream:
    rest = np.asarray([list(el) for el in resultstream.container])
    rest = rest.T
    array = [np.asarray([]) for elem in KEYLIST]
    for i, el in enumerate(rest):
        if i < len(KEYLIST) - 1:
            if i == 0:
                array[i] = num2date(el.astype(np.float64))
                array[i] = np.asarray([el.replace(tzinfo=None) for el in array[i]])
            elif KEYLIST[i + 1] in NUMKEYLIST:
                array[i] = el.astype(np.float64)
            else:
                array[i] = el
    array = np.asarray(array, dtype=object)
    resultstream = DataStream(header=resultstream.header, ndarray=array)

    for idx, elem in enumerate(resultstream.ndarray):
        if KEYLIST[idx] in NUMKEYLIST:
            A = resultstream.ndarray[idx]
            A[A==999999.99] = np.nan
            A[A==np.inf] = np.nan
            resultstream.ndarray[idx] = A
            #resultstream.ndarray[idx] = np.where(resultstream.ndarray[idx].astype(float) == 999999.99, np.nan,
            #                                     resultstream.ndarray[idx])
            #resultstream.ndarray[idx] = np.where(np.isinf(resultstream.ndarray[idx].astype(float)), np.nan,
            #                                     resultstream.ndarray[idx])

    # Add deltaF to resultsstream for all Fext:  if nan then df == deltaF else df = df+deltaF,
    posF = KEYLIST.index('str4')
    posdf = KEYLIST.index('df')

    for idx, elem in enumerate(resultstream.ndarray[posF]):
        # print elem
        # if not deltaF:
        #    deltaF = 0.0
        if not deltaF == 0 and elem.startswith('Fext'):
            try:
                resultstream.ndarray[posdf][idx] = deltaF
            except:
                array = [np.nan] * len(resultstream.ndarray[0])
                array[idx] = deltaF
                resultstream.ndarray[posdf] = np.asarray(array)
        elif not deltaF == 0 and elem.startswith('Fabs'):
            try:
                resultstream.ndarray[posdf][idx] = float(resultstream.ndarray[posdf][idx]) + float(deltaF)
            except:
                array = [np.nan] * len(resultstream.ndarray[0])
                array[idx] = deltaF
                resultstream.ndarray[posdf] = np.asarray(array)

    if not stationid and varioid == scalarid:
        stationid = varioid

    # 3.1 Header information
    # --------------------
    di_version = '1.1'
    resultstream.header['StationID'] = stationid
    resultstream.header['DataPier'] = pier
    resultstream.header['DataFormat'] = 'MagPyDI'
    resultstream.header['DataType'] = "{}{}".format('MagPyDI', di_version)
    resultstream.header['DataComponents'] = 'IDFF'
    resultstream.header['col-time'] = 'Epoch'
    resultstream.header['col-x'] = 'i'
    resultstream.header['unit-col-x'] = 'deg'
    resultstream.header['col-y'] = 'd'
    resultstream.header['unit-col-y'] = 'deg'
    resultstream.header['col-z'] = 'f'  # F into column-z to allow coordinate conversions (e.g. idf2xyz)
    resultstream.header['unit-col-z'] = 'nT'
    resultstream.header['col-f'] = 'f'
    resultstream.header['unit-col-f'] = 'nT'
    if variometerorientation in ["XYZ", "xzy"]:
        resultstream.header['col-dx'] = 'X-base'
        resultstream.header['unit-col-dx'] = 'nT'
        resultstream.header['col-dy'] = 'Y-base'
        resultstream.header['unit-col-dy'] = 'nT'
    else:
        resultstream.header['col-dx'] = 'H-base'
        resultstream.header['unit-col-dx'] = 'nT'
        resultstream.header['col-dy'] = 'D-base'
        resultstream.header['unit-col-dy'] = 'deg'
    resultstream.header['col-dz'] = 'Z-base'
    resultstream.header['unit-col-dz'] = 'nT'
    resultstream.header['col-df'] = 'dF'
    resultstream.header['col-t1'] = 'T'
    resultstream.header['unit-col-t1'] = 'deg C'
    resultstream.header['col-t2'] = 'ScaleValueDI'
    resultstream.header['unit-col-t2'] = 'deg/unit'
    resultstream.header['col-var1'] = 'Dec_S0'
    resultstream.header['col-var2'] = 'Dec_deltaH'
    resultstream.header['col-var3'] = 'Dec_epsilonZ'
    resultstream.header['col-var4'] = 'Inc_S0'
    resultstream.header['col-var5'] = 'Inc_epsilonZ'
    resultstream.header['col-str1'] = 'Person'
    resultstream.header['col-str2'] = 'DI-Inst'
    resultstream.header['col-str3'] = 'Mire'
    resultstream.header['col-str4'] = 'F-type'

    # Provide a SensorID
    resultstream.header['DataID'] = 'BLV_{}_{}_{}'.format(varioid, scalarid, pier)
    resultstream.header['SensorID'] = 'BLV_{}_{}_{}'.format(varioid, scalarid, pier)

    print("------------------------------------------------------")
    print("Failed files:")
    print("------------------------------------------------------")
    if failingdict:
        for el in failingdict:
            print(el, failingdict.get(el))
    else:
        print("None")
    print("------------------------------------------------------")

    resultstream = resultstream.sorting()

    # Apply correct format to resultsstream
    array = [[] for el in KEYLIST]
    for idx, el in enumerate(resultstream.ndarray):
        if KEYLIST[idx] in NUMKEYLIST:
            array[idx] = np.asarray(el).astype(float)
        elif 'time' in KEYLIST[idx]:
            array[idx] = np.asarray(el).astype(np.datetime64).astype(datetime)
        else:
            array[idx] = np.asarray(el)
    resultstream.ndarray = np.asarray(array, dtype=object)

    return resultstream


# ##################
# ------------------
# Main program
# ------------------
# ##################


if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: Absolutes PACKAGE")
    print("THIS IS A TEST RUN OF THE MAGPY ABSOLUTES PACKAGE.")
    print("All main methods will be tested. This may take a while.")
    print("If errors are encountered they will be listed at the end.")
    print("Otherwise True will be returned")
    print("----------------------------------------------------------")
    print()

    #import subprocess
    # #######################################################
    #                     Runtime testing
    # #######################################################

    def create_teststream(startdate=datetime(2022, 11, 21), coverage=86400):
        # Create a random data signal with some nan values in x and z
        c = 1000  # 1000 nan values are filled at random places
        l = coverage
        array = [[] for el in DataStream().KEYLIST]
        import scipy
        win = scipy.signal.windows.hann(60)
        a = np.random.uniform(20950, 21000, size=int((l + 2880) / 2))
        b = np.random.uniform(20950, 21050, size=int((l + 2880) / 2))
        x = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        array[1] = np.asarray(x[1440:-1440])
        a = np.random.uniform(1950, 2000, size=int((l + 2880) / 2))
        b = np.random.uniform(1900, 2050, size=int((l + 2880) / 2))
        y = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
        array[2] = np.asarray(y[1440:-1440])
        a = np.random.uniform(44300, 44400, size=(l + 2880))
        z = scipy.signal.convolve(a, win, mode='same') / sum(win)
        array[3] = np.asarray(z[1440:-1440])
        array[4] = np.asarray(np.sqrt((x * x) + (y * y) + (z * z))[1440:-1440])
        var1 = [0] * l
        var1[43200:50400] = [1] * 7200
        varind = DataStream().KEYLIST.index('var1')
        array[varind] = np.asarray(var1)
        array[0] = np.asarray([startdate + timedelta(seconds=i) for i in range(0, l)])
        teststream = DataStream(header={'SensorID': 'Test_0001_0001'}, ndarray=np.asarray(array, dtype=object))
        teststream.header['col-x'] = 'X'
        teststream.header['col-y'] = 'Y'
        teststream.header['col-z'] = 'Z'
        teststream.header['col-f'] = 'F'
        teststream.header['unit-col-x'] = 'nT'
        teststream.header['unit-col-y'] = 'nT'
        teststream.header['unit-col-z'] = 'nT'
        teststream.header['unit-col-f'] = 'nT'
        teststream.header['col-var1'] = 'Switch'
        teststream.header['StationID'] = 'TST'
        return teststream

    teststream1 = create_teststream(startdate=datetime(2022, 11, 22))
    teststream2 = create_teststream(startdate=datetime(2022, 11, 23))

    ok = True
    errors = {}
    successes = {}
    if ok:
        #testrun = './testflagfile.json' # define a test file later on
        t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)
        while True:
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                test = deg2degminsec(270.5)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['deg2degminsec'] = ("Version: {}: deg2degminsec {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['deg2degminsec'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with deg2degminsec.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                absdist = abs_read(example6a, output='AbsoluteDIStruct')  # should be the default
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['AbsoluteDIStruct'] = ("Version: {}: AbsoluteDIStruct {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['AbsoluteDIStruct'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with AbsoluteDIStruct.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                absst = abs_read(example6a)  # should be the default
                for ab in absst:
                    l1 = ab.get_data_list()
                    abdi = ab.get_abs_distruct()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['DILineStruct'] = ("Version: {}: DILineStruct {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['DILineStruct'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with DILineStruct.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                data = data_for_di({'file': example5}, starttime='2018-08-29', endtime='2018-08-30', datatype='both',
                                   debug=True)
                valuetest1 = abdi._check_coverage(data, keys=['f'])
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_check_coverage'] = ("Version: {}: _check_coverage {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_check_coverage'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _check_coverage.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                func = data.header.get('DataFunctionObject')[0]
                abdi = abdi._insert_function_values(func)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_insert_function_values'] = ("Version: {}: _insert_function_values {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_insert_function_values'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _insert_function_values.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                f = 50.
                h = abdi._h(f,35.)
                z = abdi._z(f,35.)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_h_z'] = ("Version: {}: _h_z {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_h_z'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _h_z.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                for key in abdi.ABSKEYLIST:
                    t = abdi._get_column(key)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_get_column'] = ("Version: {}: _get_column {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_get_column'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _get_column.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                ma = abdi._get_max('varf')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_get_max'] = ("Version: {}: _get_max {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_get_max'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _get_max.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                mi = abdi._get_min('varf')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_get_min'] = ("Version: {}: _get_min {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_get_min'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _get_min.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                resultline, decmeanx, decmeany, variocorrold = abdi._calcdec(xstart=20000,ystart=1700,hstart=0.0,hbasis=0.0,ybasis=0.0,deltaD=0.0,usestep=0,scalevalue=None,iterator=0,annualmeans=None,meantime=False,xyzorient=False,residualsign=1,debugmode=False)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_calcdec'] = ("Version: {}: _calcdec {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_calcdec'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _calcdec.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                outline, hstart, hbasis = abdi._calcinc(resultline,scalevalue=None,incstart=0.0,deltaI=0.0,iterator=0,usestep=0,annualmeans=None,xyzorient=False,decmeanx=decmeanx,decmeany=decmeany,variocorrold=variocorrold,residualsign=1,debugmode=True)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_calcinc'] = ("Version: {}: _calcinc {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_calcinc'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _calcinc.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                angle = abdi._corrangle(386.9)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_corrangle'] = ("Version: {}: _corrangle {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_corrangle'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _corrangle.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                result = abdi.calcabsolutes(usestep=0, annualmeans=None, printresults=True, debugmode=False,
                              deltaD=0.0, deltaI=0.0, meantime=False, scalevalue=None,
                              variometerorientation='hez', residualsign=1)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['calcabsolutes'] = ("Version: {}: calcabsolutes {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['calcabsolutes'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with calcabsolutes.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                from magpy.core import database
                db = database.DataBank("localhost","maxmustermann","geheim","testdb")
                absst = abs_read(example6a)
                if db:
                    db.diline_to_db(absst, mode="delete", stationid='WIC')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['diline_to_db'] = ("Version: {}: diline_to_db {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['diline_to_db'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with diline_to_db.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                from magpy.core import database
                db = database.DataBank("localhost","maxmustermann","geheim","testdb")
                if db:
                    res = db.diline_from_db()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['diline_from_db'] = ("Version: {}: diline_from_db {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['diline_from_db'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with diline_from_db.")
            try:
                print ("_analyse_di_source test")
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                from magpy.core import database
                db = database.DataBank("localhost","maxmustermann","geheim","testdb")
                absst = abs_read(example6a)  # should be the default
                print ("_analyse_di_source test: preparation done")
                if db:
                    t1, fa = _analyse_di_source('DIDATA', db=db)
                    print("_analyse_di_source test: database done")
                    t2, fa = _analyse_di_source(example6a, db=db)
                    print("_analyse_di_source test: file done")
                    t3, fa = _analyse_di_source(absst, db=db, debug=True)
                    print("_analyse_di_source test: listobject done")
                    print ("{} and {} and {} should all be 1".format(len(t1),len(t2),len(t3)))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_analyse_di_source'] = ("Version: {}: _analyse_di_source {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_analyse_di_source'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _analyse_di_source.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                data = data_for_di(example5, starttime="2018-08-29", datatype='both',debug=True)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['data_for_di'] = ("Version: {}: data_for_di {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['data_for_di'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with data_for_di.")
            baseval1 = absolute_analysis(example6a, example5, example5)
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                baseval1 = absolute_analysis(example6a, example5, example5)
                baseval2 = absolute_analysis([example6a, example6b],
                                               {'file': example5}, example5, db=db,
                                               starttime="2018-08-28", endtime="2018-08-30")
                from magpy.core import database
                db = database.DataBank("localhost","maxmustermann","geheim","testdb")
                if db:
                    data = read(example5)
                    db.write(data)
                    baseval3 = absolute_analysis(example6a, {'file':example5, 'db':(db,'WIC_1_0001_0001')}, example5, db=db, starttime="2018-08-28", endtime="2018-08-30")
                    baseval4 = absolute_analysis('DIDATA', {'file':example5, 'db':(db,'WIC_1_0001_0001')}, example5, db=db, starttime="2018-08-28", endtime="2018-08-30")
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['absolute_analysis'] = ("Version: {}: absolute_analysis {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['absolute_analysis'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with absolute_analysis.")

            # If end of routine is reached... break.
            break

        t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
        time_taken = t_end_test - t_start_test
        print(datetime.now(timezone.utc).replace(tzinfo=None), "- Database runtime testing completed in {} s. Results below.".format(time_taken.total_seconds()))

        print()
        print("----------------------------------------------------------")
        #del_test_files = 'rm {}*'.format(testrun)
        #subprocess.call(del_test_files,shell=True)
        if errors == {}:
            print("0 errors! Great! :)")
        else:
            print(len(errors), "errors were found in the following functions:")
            print(" {}".format(errors.keys()))
            print()
            for item in errors:
                    print(item + " error string:")
                    print("    " + errors.get(item))
        print()
        print("Good-bye!")
        print("----------------------------------------------------------")
