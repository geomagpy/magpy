#!/usr/bin/env python
"""
MagPy-absolutes:
Analysis of DI data.
Uses mainly two classes
- DILineStruct (can contain multiple DI measurements; used for storage, database com and autodif)
- AbsStruct (contains a single measurement, is extended by variation and scalar data, used for calculation)
Calculation returns a data stream with absolute and baseline values for any selected variometer

TEST REPORT in GERMAN/ENGLISH MIX:
Systematische Tests:
1.  (11/2013, OK) Laden von Raw-Datem
2.  Laden von alten Raw-Daten
3.  (11/2013, OK) Laden von AutoDIF Daten
4.  (11/2013, OK) Auswertung von gon/deg Daten
5.  (11/2013, OK) Richtigkeit von Gon-Auswertungen: Vergleich mit Juergens Excel
6.  (11/2013, OK) Auswertung ohne Vario und/oder Scalardaten
7.  (11/2013, OK) Systematischer Test - Einfluss der Variometerkorrektur (Korrektur mit basis und nicht basiskorr Variation - sollte identisch sein, korrektur mit unterschiedlichen Varios)
8.  (11/2013, OK) Systematischer Test der Winkelabhaengigkeit (AutoDiff - Bestimme Tagesvariation in Abhaengigkeit von verschiedenen Winkeln)
9.  (11/2013, not necessary: raw data is always available and the file wold not contain any additional info - use database if comments are required) Laden und Speichern von DIListStructs
10. (11/2013, OK) Datenbankspeicherung
11. Create a direct DataBase input formular in php (and magpy-gui?? -> better refer to php there)
11. (12/2013, not necessary) Laden von mehreren Files gleichzeitig (mit DILineStruct) - not really necessary
12. Basislinien-Stabilitaet (Berechnung der Zuverlaessigkeit der Basislinien-Extrapolation)
13. Write the documentation with citations of the underlying procedures
14. (12/2013, OK) Show Stereoplot
15. Briefly descripe some related bin files

zu 8:
look at bin_angulardependency.py

Related bins:
bin_absolute (analyse DI measurements)
bin_angulardependency (test the influence of variometerorientation several DI measurements during a day (e.g. autodif))
bin_baselinestability (too be written)


Written by Roman Leonhardt 2011/2012/2013
Version 1.0 (from the 23.02.2012)

"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division


from magpy.stream import *
from magpy.database import *
from magpy.transfer import *

import dateutil.parser as dparser

MAGPY_SUPPORTED_ABSOLUTES_FORMATS = ['MAGPYABS','MAGPYNEWABS','JSONABS','AUTODIFABS','AUTODIF','UNKNOWN']
ABSKEYLIST = ['time', 'hc', 'vc', 'res', 'f', 'mu', 'md', 'expectedmire', 'varx', 'vary', 'varz', 'varf', 'var1', 'var2']

miredict = {'UA': 290.0, 'MireTower':41.80333,'MireChurch':51.1831,'MireCobenzl':353.698}

class AbsoluteDIStruct(object):
    def __init__(self, time=0, hc=float('nan'), vc=float('nan'), res=float('nan'), f=float('nan'), mu=float('nan'), md=float('nan'), expectedmire=float('nan'),varx=float('nan'), vary=float('nan'), varz=float('nan'), varf=float('nan'), var1=float('nan'), var2=float('nan'), temp=float('nan'), person='', di_inst='', f_inst=''):
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
    def __init__(self, ndi, nf=0, time=float('nan'), laser=float('nan'), hc=float('nan'), vc=float('nan'), res=float('nan'), ftime=float('nan'), f=float('nan'), opt=float('nan'), t=float('nan'), scaleflux=float('nan'), scaleangle=float('nan'), azimuth=float('nan'),
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

    def getDataList(self):
        """
        DEFINITION:
            convert the DILineStruct to a Datalist used for displaying and saving

        RESULT:
            #['# MagPy Absolutes\n', '# Abs-Observer: Leichter\n', '# Abs-Theodolite: T10B_0619H154167_07-2011\n', '# Abs-TheoUnit: deg\n', '# Abs-FGSensor: MAG01H_SerialSensor_SerialElectronic_07-2011\n', '# Abs-AzimuthMark: 180.1044444\n', '# Abs-Pillar: A4\n', '# Abs-Scalar: /\n', '# Abs-Temperature: 6.7C\n', '# Abs-InputDate: 2016-01-26\n', 'Miren:\n', '0.099166666666667  0.098055555555556  180.09916666667  180.09916666667  0.098055555555556  0.096666666666667  180.09805555556  180.09805555556\n', 'Positions:\n', '2016-01-21_13:22:00  93.870555555556  90  1.1\n', '2016-01-21_13:22:30  93.870555555556  90  1.8\n', '2016-01-21_13:27:00  273.85666666667  90  0.1\n', '2016-01-21_13:27:30  273.85666666667  90  0.2\n', '2016-01-21_13:25:30  273.85666666667  270  0.3\n', '2016-01-21_13:26:00  273.85666666667  270  -0.6\n', '2016-01-21_13:24:00  93.845555555556  270  -0.2\n', '2016-01-21_13:24:30  93.845555555556  270  0.4\n', '2016-01-21_13:39:30  0  64.340555555556  -0.3\n', '2016-01-21_13:40:00  0  64.340555555556  0.1\n', '2016-01-21_13:38:00  0  244.34055555556  0\n', '2016-01-21_13:38:30  0  244.34055555556  -0.4\n', '2016-01-21_13:36:00  180  295.67055555556  1.1\n', '2016-01-21_13:36:30  180  295.67055555556  1.2\n', '2016-01-21_13:34:30  180  115.66916666667  0.3\n', '2016-01-21_13:35:00  180  115.66916666667  0.9\n', '2016-01-21_13:34:30  180  115.66916666667  0\n', 'PPM:\n', 'Result:\n']

        EXAMPLE:
            >>> abslinestruct.getAbsDIStruct()
        """
        try:
            mu1 = self.hc[0]-((self.hc[0]-self.hc[1])/(self.laser[0]-self.laser[1]))*self.laser[0]
            if isnan(mu1):
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
                self.inputdate = datetime.utcnow()
        datalist.append('# Abs-InputDate: {}\n'.format(datetime.strftime(self.inputdate,"%Y-%m-%d")))

        datalist.append('Miren:\n')
        datalist.append('{}  {}  {}  {}  {}  {}  {}  {}\n'.format(md1, md1, mu1, mu1, md2, md2, mu2, mu2))
        datalist.append('Positions:\n')
        absst = self.getAbsDIStruct()
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


    def getAbsDIStruct(self):
        """
        DEFINITION:
            convert the DILineStruct to the AbsDataStruct used for calculations

        EXAMPLE:
            >>> abslinestruct.getAbsDIStruct()
        """
        try:
            mu1 = self.hc[0]-((self.hc[0]-self.hc[1])/(self.laser[0]-self.laser[1]))*self.laser[0]
            if isnan(mu1):
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
            headers['analysisdate'] = self.time[0]
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

        #print sortlist

        ## Eventually append f values

        for elem in sortlist:
            stream.add(elem)
        return stream


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
        if angle > 360:
            angle = angle - 360.0
        elif angle < 0:
            angle = angle + 360.0
        return angle


    # ----------------
    # internal methods
    # ----------------

    def _get_max(self, key):
        if not key in ABSKEYLIST:
            raise ValueError("Column key not valid")
        elem = max(self, key=lambda tmp: eval('tmp.'+key))
        return eval('elem.'+key)


    def _get_min(self, key):
        if not key in ABSKEYLIST:
            raise ValueError("Column key not valid")
        elem = min(self, key=lambda tmp: eval('tmp.'+key))
        return eval('elem.'+key)


    def _get_column(self, key):
        """
        returns an numpy array of selected columns from Stream
        """
        if not key in ABSKEYLIST:
            raise ValueError("Column key not valid")

        return np.asarray([eval('elem.'+key) for elem in self])

    def _check_coverage(self,datastream, keys=['x','y','z']):
        """
        DEFINITION:
            check whether variometer/scalar data is available for each time step
            of DI data (within 2* samplingrate diff)
        """
        # 1. Drop all data without value
        for key in keys:
            datastream = datastream._drop_nans(key)
        samprate = datastream.samplingrate()
        if not datastream.length()[0] > 1:
            return False

        # 2. Get time column
        timea = np.asarray(datastream._get_column('time'))

        # 3. Get time column of DI data
        timeb = np.asarray([el.time for el in self])
        # 4. search
        # corrected in version 0.9.9
        indtia = [idx for idx, el in enumerate(timeb) if np.min(np.abs(timea-float(el)))/((samprate/24./3600.)*2) <= 1.]
        if not len(indtia) == len(timeb):
            print ("_check_coverage: timesteps of scalar data are off by more than twice the sampling rate from DI measurements")
            return False

        return True

    def _insert_function_values(self, function,**kwargs):
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
            >>> abstream._calcdec()
        """

        funckeys = kwargs.get('funckeys')
        offset = kwargs.get('offset')
        debug = kwargs.get('debug')
        if not funckeys:
            funckeys = ['x','y','z','f']
        if not offset:
            offset = 0.0

        for elem in self:
            # check whether time step is in function range
            if function[1] <= elem.time <= function[2]:
                functime = (elem.time-function[1])/(function[2]-function[1])
                #if debug:
                #    print ("Inserting at {}:".format(num2date(functime)))
                for key in funckeys:
                    if not key in KEYLIST[1:15]:
                        raise ValueError("Column key not valid")
                    fkey = 'f'+key
                    if fkey in function[0]:
                        try:
                            newval = float(function[0][fkey](functime)) + offset
                        except:
                            newval = float('nan')
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
            >>> abstream._calcdec()
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
        if poslst[1].mu-5 < (miremean-90.0) < poslst[1].mu+5:
            mireval = miremean-90.0
        else:
            mireval = miremean+90.0
        mirediff = self._corrangle(expmire - mireval)

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
                    loggerabs.error("_calcdec:  %s : Data missing: check whether all fields are filled"% num2date(poslst[0].time).replace(tzinfo=None))
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

        #print("_calcdec:  Dec calc: %f, %f, %f, %f" % (decmean, mirediff, variocorr[0], deltaD))
        #print ("Hallo", decmean, mirediff, variocorr[determinationindex]*180.0/np.pi, deltaD)

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
            #print ("deH", (-dl2tmp[0]-dl2tmp[1]+dl2tmp[2]+dl2tmp[3])/4)
            if debugmode:
                print ("_calcdec:  collimation angle (dl2tmp): %f, %f, %f, %f; hstart: %f" % (dl2tmp[0],dl2tmp[1],dl2tmp[2],dl2tmp[3],hstart))
            loggerabs.debug("_calcdec:  collimation angle (dl2tmp): %f, %f, %f, %f; hstart: %f" % (dl2tmp[0],dl2tmp[1],dl2tmp[2],dl2tmp[3],hstart))
            if dl2tmp[0]<dl2tmp[1]:
                epZD = (dl2tmp[0]-dl2tmp[1]-dl2tmp[2]+dl2tmp[3]+2*np.pi)/4*hstart
            else:
                epZD = (dl2tmp[0]-dl2tmp[1]-dl2tmp[2]+dl2tmp[3]-2*np.pi)/4*hstart
        else:
            s0d = float('nan')
            deH = float('nan')
            epZD = float('nan')

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
            >>> abstream._calcinc()
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
            """
            print ("Hungarian sheet - only using F during Inc")
            if len(variox) >= 16:
                # Correct variant: select means for measurements during vc
                meanvariox = np.mean(variox[8:16])
                meanvarioy = np.mean(varioy[8:16])
                meanvarioz = np.mean(varioz[8:16])
                meanf = np.mean(fvlist[8:16])
                print ("Dont do that - use original way")
            else:
            """
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
            deltaF = float('nan')

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
                    ppmtmp = float('nan')
                else:
                    # correctness the following line requires that meanf is determined in the same time range as meanvario comps
                    # other sheets only take meanf and variomeans only during the respective cycle - with constant delta F
                    ppmtmp = meanf + (scale_x*poslst[k].varx - meanvariox)*np.cos(incstart*np.pi/180.) + (scale_z*poslst[k].varz - meanvarioz)*np.sin(incstart*np.pi/180.) + ((scale_y*poslst[k].vary)**2-(meanvarioy)**2)/(2*meanf)
                    #print ("Getting variationcorrected F value for each Hv measurement:", ppmtmp)
                    #print (" consisting of ", meanf, (scale_x*poslst[k].varx - meanvariox)*np.cos(incstart*np.pi/180.), (scale_z*poslst[k].varz - meanvarioz)*np.sin(incstart*np.pi/180.),((scale_y*poslst[k].vary)**2-(meanvarioy)**2)/(2*meanf))
                    #print ("scales", scale_x,scale_y,scale_z, incstart)
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
            #print (" Residual correction", poslst[k].res, rcorri)
            # The signums are essential for the collimation angle calculation
            # test_di: also checks regarding correctness for different location/angles
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            #signum1 = np.sign(np.tan(quad*np.pi/180))
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

            #print ("I0:", I0*180.0/np.pi)
            # previous version -- I0 = (signum1*poslst[k].vc*np.pi/180.0 - signum2*rcorri - signum1*PiVal)

            #print ("Inc:", signum1*poslst[k].vc*200/180, quad, I0*200./np.pi, rcorri*200./np.pi, signum2, PiVal, ppmval[cnt])

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
                            fieldchange = float('nan')
                        else:
                            fieldchange = (-np.sin(np.mean(I0list))*(poslst[n].varx-poslst[k].varx)/mean(ppmval) + np.cos(np.mean(I0list))*(poslst[n].varz-poslst[k].varz)/mean(ppmval))*180/np.pi
                        deltaB = rotation+fieldchange
                        deltaR = np.abs(poslst[n].res-poslst[k].res)
                        minimum = rotation
                        if (deltaR == 0):
                            calcscaleval = 999.0
                        else:
                            #print "Scaleval", mean(ppmval), deltaB, deltaR, rotation, fieldchange
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
        if len(xvals) > 8: # drop value from scale measurement
            xvals = xvals[:8]
            yvals = yvals[:8]
            zvals = zvals[:8]


        #avcorrf =  mean(ppmval) # definitely not meanf, evetually use ppmval[0] to pick first time step
        meanf =  mean(selppmval)
        tmpH = self._h(meanf, inc)
        tmpZ = self._z(meanf, inc)
        dec = linestruct.y
        if xyzorient:
            # for xyz use the average variocorrected declination value from the hc measurements
            dec2 = variocorrold
        else:
            # for hdz base value and dec are determined at the initial time step
            dec2 = dec
            #pass
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
            s0i, epzi = float('nan'),float('nan')
            fstart, deltaF = float('nan'),float('nan')
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
                h_adder = float('nan')
                z_adder = float('nan')
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
        """
        if debugmode:
            #1.) get x,y,z from i,d,f
            xyz = coordinate([inc,linestruct.y,fstart])
            #2.) Baselinevalues:
            bax = xyz[0] - scale_x*poslst[0].varx
            bay = xyz[1] - scale_y*poslst[0].vary
            baz = xyz[2] - scale_z*poslst[0].varz
            bax2 = xyz[0] - mean(xvals)
            bay2 = xyz[1] - mean(yvals)
            baz2 = xyz[2] - mean(zvals)
            print ("Variometer values 1:", poslst[0].varx, poslst[0].vary, poslst[0].varz)
            print ("Variometer values 2:", mean(xvals),mean(yvals),mean(zvals))
            print ("Basevalues", bax1,bay1,baz1, bax2,bay2,baz2)
            #baseh = np.sqrt(basex**2+basey**2)
        """
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
            >>> stream.calcabsolutes(usestep=2,annualmeans=[20000,1200,43000],printresults=True)

        APPLICATION:
            absst = absRead(path_or_url=abspath,azimuth=azimuth,output='DIListStruct')
            stream = elem.getAbsDIStruct()
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
            if isnan(resultline.y):
                try:
                    loggerabs.error('%s : CalcAbsolutes: Declination could not be determined - aborting' % num2date(self[0].time).replace(tzinfo=None))
                except:
                    loggerabs.error('unkown : CalcAbsolutes: Declination could not be determined - aborting')
                break
            # use incstart and ystart as boundary conditions
            try: # check, whether outline already exists
                if isnan(outline.x):
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
        for key in FLAGKEYLIST:
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
                print('Vector at: {}'.format(str(num2date(outline.time))))
                print('Declination: %s, Inclination: %s, H: %.1f, Z: %.1f, F: %.1f' % (deg2degminsec(outline.y),deg2degminsec(outline.x),outline.f*np.cos(outline.x*np.pi/180),outline.f*np.sin(outline.x*np.pi/180),outline.f))
                print('Collimation and Offset:')
                print('Declination:    S0: %.3f, delta H: %.3f, epsilon Z: %.3f\nInclination:    S0: %.3f, epsilon Z: %.3f\nScalevalue: %.3f deg/unit' % (outline.var1,outline.var2,outline.var3,outline.var4,outline.var5,outline.t2))
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


def _logfile_len(fname, logfilter):
    f = open(fname,"rb")
    cnt = 0
    for line in f:
        if logfilter in line:
            cnt = cnt +1
    return cnt


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


def absRead(path_or_url=None, dataformat=None, headonly=False, **kwargs):
    """
    optional keywords:
    username: for authentication in ftp and ssh access
    password: for authentication (following the example of http://www.voidspace.org.uk/python/articles/authentication.shtml
    """
    # Not used so far - maybe for simplifying ftp or ssh
    username = kwargs.get('username')
    password = kwargs.get('password')
    archivepath = kwargs.get('archivepath')
    output = kwargs.get('output')

    # -- No path
    if not path_or_url:
        messagecont = "File not specified"
        return [],messagecont
    # -- Create Data Container
    stream = AbsoluteData()
    # -- Check file
    if not isinstance(path_or_url, basestring):
        # not a string - we assume a file-like object
        pass
    elif "://" in path_or_url:
        # some URL
        #proxy_handler = urllib2.ProxyHandler( {'http': '138.22.156.44:8080', 'https' : '138.22.156.44:443', 'ftp' : '138.22.156.44:8021' } )
        #opener = urllib2.build_opener(proxy_handler,urllib2.HTTPBasicAuthHandler(),urllib2.HTTPHandler, urllib2.HTTPSHandler,urllib2.FTPHandler)
        # install this opener
        #urllib2.install_opener(opener)

        # extract extension if any
        suffix = '.'+os.path.basename(path_or_url).partition('.')[2] or '.tmp'
        name = os.path.basename(path_or_url).partition('.')[0] # append the full filename to the temporary file
        fh = NamedTemporaryFile(suffix=name+suffix,delete=False)
        fh.write(urllib2.urlopen(path_or_url).read())
        fh.close()
        stream = _absRead(fh.name, dataformat, headonly, **kwargs)
        os.remove(fh.name)
    else:
        # some file name
        pathname = path_or_url
        stream = _absRead(pathname, dataformat, headonly, **kwargs)
        if len(stream) == 0:
            # try to give more specific information why the stream is empty
            if has_magic(pathname) and not glob(pathname):
                raise Exception("No file matching file pattern: %s" % pathname)
            elif not has_magic(pathname) and not os.path.isfile(pathname):
                raise IOError(2, "No such file or directory", pathname)
            # Only raise error if no starttime/endtime has been set. This
            # will return an empty stream if the user chose a time window with
            # no data in it.
            # XXX: Might cause problems if the data is faulty and the user
            # set starttime/endtime. Not sure what to do in this case.
            #elif not 'starttime' in kwargs and not 'endtime' in kwargs:
            #    raise Exception("Cannot open file/files: %s" % pathname)

    return stream

def _absRead(filename, dataformat=None, headonly=False, **kwargs):
    """
    Reads a single file into a ObsPy Stream object.
    """

    output = kwargs.get('output')
    # get format type
    format_type = None
    if not dataformat:
        # auto detect format - go through all known formats in given sort order
        for format_type in MAGPY_SUPPORTED_ABSOLUTES_FORMATS:
            # check format
            if isAbsFormat(filename, format_type):
                break
    else:
        # format given via argument
        dataformat = dataformat.upper()
        try:
            formats = [el for el in MAGPY_SUPPORTED_ABSOLUTES_FORMATS if el == dataformat]
            format_type = formats[0]
        except IndexError:
            msg = "Format \"%s\" is not supported. Supported types: %s"
            raise TypeError(msg % (dataformat, ', '.join(MAGPY_SUPPORTED_ABSOLUTES_FORMATS)))
    # file format should be known by now
    print("DI format:", format_type)

    stream = readAbsFormat(filename, format_type, headonly=headonly, **kwargs)

    # If stream could not be read return an empty datastream object
    try:
        xxx = len(stream)
    except:
        stream = DataStream()

    return stream


def absoluteAnalysis(absdata, variodata, scalardata, **kwargs):
    """
    DEFINITION:
        Analyze absolute data from files or database and create datastream

    PARAMETERS:
    Variables:
        - header_dict:  (dict) dictionary with header information
        variodata:      (string) path to variodata, can include wildcards like /my/path/*.sec
        scalardata:     (string) path to scalardata, can include wildcards like /my/path/*.sec
    Kwargs:
        - starttime:    (string/datetime) define begin
        - endtime:      (string/datetime) define end
        - abstype:      (string) default manual, can be autodif
        - db:           (mysql database) defined by mysql.connect().
        - dbadd:        (bool) if True DI-raw data will be added to the database
        - alpha:        (float) orientation angle 1 in deg (if z is vertical, alpha is the horizontal rotation angle)
        - beta:         (float) orientation angle 2 in deg
        - deltaF:       (float) difference between scalar measurement point and pier (F(DI-flux pier) - F(scalar pier))
        - deltaD:       (float) = kwargs.get('deltaD')
        - deltaI:       (float) = kwargs.get('deltaI')
        - diid:         (string) identifier (id) of di files (e.g. A2_WIC.txt, don't use wildcards like *)
        - outputformat: (string) one of 'idf', 'xyz', 'hdf'
        - usestep:      (int) which step to use for analysis, usually both, in autodif only 2
        - annualmeans:  (list) provide annualmean for x,y,z as [x,y,z] with floats
        - azimuth:      (float) required for Autodif measurements
        - variometerorientation: (string) accepts XYZ, xyz, in any other case HEZ is asumed
        - expD:         (float) expected Declination - failure produced when D differs by more than expT deg
        - expI:         (float) expected Inclination - failure produced when I differs by more than expT deg
        - expT:         (float) expected value threshold - default 1 deg
        - movetoarchive:(string) define a local directory to store archived data (only works when reading files)
    RETURNS:
        --
    EXAMPLE:
        (1) >>> stream = absoluteAnalysis('/home/leon/Dropbox/Daten/Magnetism/DI-WIC/autodif',variopath,'',abstype='autodif',azimuth=267.4242,starttime='2014-02-10',endtime='2014-02-25')
        (2) >>> stream = absoluteAnalysis('DIDATA_WIK',variopath,'',starttime='2013-12-20',endtime='2013-12-30',db=db)
        (3) >>> stream = absoluteAnalysis('/home/leon/Dropbox/Daten/Magnetism/DI-WIC/raw/',variopath,scalarpath,diid='A2_WIC.txt',starttime='2014-02-10',endtime='2014-02-25',db=db,dbadd=True)
        (3) >>> stream = absoluteAnalysis('http://localhost/mydata.html',variopath,scalarpath,diid='A2_WIC.txt',db=db,dbadd=True)

    PERFORMED TESTS:
        (OK)1. Read data from database and files
        (OK)2. Read data from autodif and manual analysis
        (OK)3. Put data to database (dbadd)
        (OK)4. Check parameter for all possibilities: a) no db, b) db, c) db and override by input
        (OK)5. Archiving function
        6. Appropriate information on failed analyses -> add test for expected values
        (OK)7. is the usestep variable correctly applied for autodif and normal?
        (OK)8. overwrite of existing database lines?
        (OK)9. Order of saving data when analyzing older data sets - requires reload and delete
        10. Test memory capabilities for large data sets (~ 6month with two varios, one scalar, no vario and scalar data)
        (OK)11. Check URL/FTP read, single file and directory read

        Bugs:
        when importing db2diline from database, the DILineStruct, defined in absolutes is missing.... why???????? see Test4 in FullAbsoluteAnalysis -- order of import is very important

    """
    db = kwargs.get('db')
    dbadd = kwargs.get('dbadd')
    skipvariodb = kwargs.get('skipvariodb')
    skipscalardb = kwargs.get('skipscalardb')
    magrotation = kwargs.get('magrotation') ### if true then compensation fields are applied
    compensation = kwargs.get('compensation') ### if true then compensation fields are applied
    alpha = kwargs.get('alpha')
    offset = kwargs.get('offset')
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    beta = kwargs.get('beta')
    stationid = kwargs.get('stationid')
    pier = kwargs.get('pier')
    deltaF = kwargs.get('deltaF')
    deltaD = kwargs.get('deltaD')
    deltaI = kwargs.get('deltaI')
    diid = kwargs.get('diid')
    outputformat = kwargs.get('outputformat')
    usestep = kwargs.get('usestep')
    annualmeans = kwargs.get('annualmeans')
    variometerorientation = kwargs.get('variometerorientation')
    scalevalue = kwargs.get('scalevalue')
    azimuth = kwargs.get('azimuth') # 267.4242 # A16 to refelctor
    abstype = kwargs.get('abstype')
    expT = kwargs.get('expT')
    expI = kwargs.get('expI')
    expD = kwargs.get('expD')
    meantime = kwargs.get('meantime')
    movetoarchive = kwargs.get('movetoarchive')
    absstruct = kwargs.get('absstruct')
    residualsign = kwargs.get('residualsign')
    debug = kwargs.get('debug')

    #residualsign=-1
    #debug=True
    if not outputformat:
        outputformat='idf'
    if not annualmeans:
        #annualmeans=[20000,1200,43000]
        annualmeans=[0.0,0.0,0.0]
    if not variometerorientation:
        variometerorientation="HEZ"
    if not abstype:
        abstype = "manual"
    if abstype == 'autodif': # we also check the person given in the file. if this is AutoDIF abstype will be set correctly automatically
        usestep=2
        if not azimuth:
            print("Azimuth needs ro be provided for AutoDIF measurements")
            return
    if not expT:
        expT = 1
    if not deltaF:
        deltaF=0.0
    if not stationid:
        stationid=''
    if not pier:
        pier=''
    if not diid:
        diid=".txt"
    if not residualsign:
        residualsign = 1

    varioid = 'Unknown'
    scalarid = 'Unknown'
    # ####################################
    # 2. Get absolute data
    # ####################################

    # 2.1 Database or File -> Get a datelist
    # --------------------
    readfile = True
    filelist, datelist = [],[]
    failinglist = []
    successlist = []
    difiles = []
    if db:
        import magpy.database as dbase

    if db and not absstruct:
        #print("absoluteAnalysis:  You selected a DB. Tyring to import database methods")
        try:
            import magpy.database as dbase
            #from magpy.database import diline2db, db2diline, readDB, applyDeltas, db2flaglist, string2dict
        except:
            print("absoluteAnalysis:  import of database methods failed - skipping eventually selected option dbadd")
            dbadd = False
        cursor = db.cursor()
        # Check whether absdata exists as table
        #print("absoluteAnalysis:  Tyring to interprete the didata path as DB Table")
        cursor.execute("SHOW TABLES LIKE '%s'" % absdata)
        try:
            value = cursor.fetchone()[0]
            # Found a table - now reading it and setting file read to zero
            try:
                sql = "SELECT StartTime FROM " + value
                cursor.execute(sql)
                values = cursor.fetchall()
                lst = [elem[0].split()[0] for elem in values]
                datelist = list(set(lst))
            except:
                print("absoluteAnalysis: Error while getting Absdata from db")
                return
            # DI TABLE FOUND
            readfile = False
            print("absoluteAnalysis:  getting DI data from database")
        except:
            print("absoluteAnalysis:  getting DI data from files")
            pass

    if absstruct:
        """
        assume that the provided absdata is already a diline structure
        """
        print ("Absolut data directly provided")
        datelist = sort(list(set([datetime.strftime(num2date(np.nanmean(el.time)).replace(tzinfo=None),"%Y-%m-%d") for el in absdata])))
        readfile = False

    if readfile:
        # Get list of files
        if isinstance(absdata, basestring):
            if "://" in absdata:
                if debug:
                    print("absolute Analysis: Found URL code - requires name of data set with date")
                if "observation.json" in absdata:
                    dataformat = 'JSONABS'
                filelist.append(absdata)
                movetoarchive = False # XXX No archiving function supported so far - will be done as soon as writing to files is available
            elif os.path.isfile(absdata):
                if debug:
                    print("absolute Analysis: Found single file")
                filelist.append(absdata)
            else:
                if debug:
                    print("absolute Analysis: Found directory")
                if os.path.exists(absdata):
                    pass
                else:
                    print("absolute Analysis: Error - path to absdata not existing: %s" % absdata)
                    sys.exit()
                for file in os.listdir(absdata):
                    if debug:
                        print("   - scanning for {} (do not include wildcards)".format(diid))
                    if file.endswith(diid):
                        filelist.append(os.path.join(absdata,file))
        else:
            try:
                #print ("Found List")
                listlen = len(absdata)
                for elem in absdata:
                    if "://" in elem:
                        if "observation.json" in elem:
                            dataformat = 'JSONABS'
                        print("Found URL code - requires name of data set with date")
                        filelist.append(elem)
                        movetoarchive = False # XXX No archiving function supported so far - will be done as soon as writing to files is available
                    elif os.path.isfile(elem):
                        #print "Found single file"
                        filelist.append(elem)
            except:
                print("Could not interpret absdata")
                return

        if debug:
            print("Files:", filelist)

        for elem in filelist:
            head, tail = os.path.split(elem)
            try:
                if elem.endswith('.json'):
                    data = readJSONABS(elem)
                    for dat in data:
                        stream = dat.getAbsDIStruct()
                        datelist.append(datetime.strftime(num2date(stream[0].time).replace(tzinfo=None),"%Y-%m-%d"))
                else:
                    date = dparser.parse(tail,fuzzy=True)
                    datelist.append(datetime.strftime(date,"%Y-%m-%d"))
            except:
                try:
                    date = dparser.parse(tail[:19],fuzzy=True)
                    datelist.append(datetime.strftime(date,"%Y-%m-%d"))
                except:
                    print("absoluteAnalysis: Found date problem in file: %s" % tail)
                    failinglist.append(elem)

        datelist = list(set(datelist))


    datetimelist = [datetime.strptime(elem,'%Y-%m-%d') for elem in datelist]
    if debug:
        print ("Datetime list of data to deal with:", datetimelist)

    empty = DataStream()
    if starttime:
        datetimelist = [elem for elem in datetimelist if elem.date() >= empty._testtime(starttime).date()]
    if endtime:
        datetimelist = [elem for elem in datetimelist if elem.date() <= empty._testtime(endtime).date()]

    if not len(datetimelist) > 0:
        print("absoluteAnalysis: No matching dates found - aborting")
        return

    def checkURL(url, starttime):
        if "://" in url:
            print (url)
            ind = url.find('starttime')
            if ind < 0:
                st = datetime.strftime(starttime,"%Y-%m-%dT%H:%M:%SZ")
                et = datetime.strftime(starttime+timedelta(days=1),"%Y-%m-%dT%H:%M:%SZ")
                url = "{}&starttime={}&endtime={}".format(url,st, et)
        return url

        # Please Note for pier information always the existing pier in the file is used

    # 2.2 Cycle through datetimelist
    # --------------------
    # read varios, scalar and all absfiles of one day
    # analyze data for each day and append results to a resultstream
    # XXX possible issues: an absolute measurement which is performed in two day (e.g. across midnight)

    resultstream = DataStream()
    for date in sorted(datetimelist):
        variofound = True
        scalarfound = True
        print("")
        print("------------------------------------------------------")
        print("Starting analysis for ", date)
        print("------------------------------------------------------")
        # a) Read variodata
        try:
            valalpha = ''
            valbeta = ''
            if not isinstance(variodata, list):
                # Varioinformation from DB can be a comma separated string -> convert to list
                # or directly be a list
                variodbtest = variodata.split(',')
            else:
                variodbtest = variodata

            if len(variodbtest) == 2:
                import magpy.database as dbase
                variostr = dbase.readDB(variodbtest[0],variodbtest[1],starttime=date,endtime=date+timedelta(days=1))
            else:
                variomod = checkURL(variodata, date)
                variostr = read(variomod,starttime=date,endtime=date+timedelta(days=1))
            print("Length of Variodata ({}): {}".format(variodbtest[-1],variostr.length()[0]))
            # ---------------------------------------
            # 1.1 Variometer data read done
            # ---------------------------------------
            # Variometer data needs to be available as xyz in nT
            # Get current components
            components = variostr.header.get('DataComponents',[])
            #if debug:
            print (" - variometer data contains the following components: {}".format(components))
            if len(components)>3:
                if components[:3] == 'HDZ':
                    print ("  Variationdata as HDZ -> converting to XYZ")
                    variostr = variostr._convertstream('hdz2xyz')
                elif components[:3] == 'IDF':
                    print ("  Variationdata as IDF -> converting to XYZ")
                    variostr = variostr._convertstream('idf2xyz')
            components2deal = variostr.header.get('DataComponents')
            if len(components2deal)>=3:
                variocomps = variostr.header.get('DataComponents')[:3].lower()
                if variocomps.startswith("xyz") and not variometerorientation.lower()=="xyz":
                    print ("  Variometer data provided in XYZ, Basevalue output projected in HDZ, however,")
                    print ("  as variometerorientation is not manually confirmed to be xyz (see manual) ")
                elif not variocomps.startswith("xyz") and variometerorientation.lower()=="xyz":
                    print("  Basevalue output projected in XYZ but variometer data provided in HEZ!")
                    print("  MagPy does not yet support that yet - switching to HDZ basevalues")
                    variometerorientation = "HEZ"
            # ---------------------------------------
            # 1.2 Transformation to nT vector done
            # ---------------------------------------
            if not variostr.header.get('SensorID') == '':
                 varioid = variostr.header.get('SensorID')
            if db and not skipvariodb:
                try:
                    vaflaglist = dbase.db2flaglist(db,variostr.header['SensorID'])
                    variostr = variostr.flag(vaflaglist)
                    print("Obtained flagging information for vario data from data base: {} flags".format(len(vaflaglist)))
                except:
                    print("Could not find flagging data in database")
                try:
                    print("Obtaining variometers meta information from db")
                    variostr.header = dbase.dbfields2dict(db,variostr.header['SensorID']+'_0001')
                except:
                    print("Failed to obtain header information from data base")
                try:
                    print("Applying delta values from db ...")
                    variostr = dbase.applyDeltas(db,variostr)
                except:
                    print("Applying delta values failed")
            # ---------------------------------------
            # 1.3 IF DB: applied DB header, DB delta values and DB flaglist to stream
            # ---------------------------------------
            if magrotation or compensation:
                print("absoluteAnalysis: Applying compensation fields to variometer data ...")
                deltasapplied = int(variostr.header.get('DataDeltaValuesApplied',0))

                try:   # Compensation values are essential for correct rotation estimates
                    if not offset and not deltasapplied == 1:  # if offset is provided then it overrides DB contents
                        print("Compensation values:")
                        if db:
                            print (" from db:")
                        offdict = {}
                        xcomp = variostr.header.get('DataCompensationX','0')
                        ycomp = variostr.header.get('DataCompensationY','0')
                        zcomp = variostr.header.get('DataCompensationZ','0')
                        if not float(xcomp)==0.:
                            offdict['x'] = -1*float(xcomp)*1000.
                        if not float(ycomp)==0.:
                            offdict['y'] = -1*float(ycomp)*1000.
                        if not float(zcomp)==0.:
                            offdict['z'] = -1*float(zcomp)*1000.
                        print (' -- applying compensation fields: x={}, y={}, z={}'.format(xcomp,ycomp,zcomp))
                        variostr = variostr.offset(offdict)
                except:
                    print("Applying compensation values failed")
            # ---------------------------------------
            # 1.4 IF rot or comp: applied bias fields from header
            # ---------------------------------------
            if db and magrotation:
                try:
                    if not alpha:
                        print("Taking rotation parameters from db... (alpha)")
                        rotstring = variostr.header.get('DataRotationAlpha','')
                        rotdict = dbase.string2dict(rotstring,typ='oldlist')
                        #print ("Dealing with year", date.year)
                        valalpha = rotdict.get(str(date.year),'')
                        if valalpha == '':
                            print (" -- no alpha value found for year {}".format(date.year))
                            maxkey = max([int(k) for k in rotdict])
                            valalpha = rotdict.get(str(maxkey),0)
                            print (" -- using alpha for year {}".format(str(maxkey)))
                        valalpha = float(valalpha)
                        if not float(valalpha)==0.:
                            print(" -- rotating with alpha: {a} degree (year {b})".format(a=valalpha,b=date.year))
                            variostr=variostr.rotation(alpha=float(valalpha))
                    else:
                        # Using manually provided rotation value - see below
                        pass
                    if not beta:
                        print("Taking rotation parameters from db... (beta)")
                        rotstring = variostr.header.get('DataRotationBeta','')
                        rotdict = dbase.string2dict(rotstring,typ='oldlist')
                        valbeta = rotdict.get(str(date.year),'')
                        if valbeta == '':
                            maxkey = max([int(k) for k in rotdict])
                            beta = rotdict[str(maxkey)]
                        valbeta = float(valbeta)
                        if not float(valbeta)==0.:
                            print("-- rotating with beta: {a} degree (year {b})".format(a=valbeta,b=date.year))
                            variostr=variostr.rotation(beta=float(valbeta))
                    else:
                        # Using manually provided rotation value - see below
                        pass
                except:
                    print("Applying rotation parameters failed")
            # ---------------------------------------
            # 1.5 IF DB and rot: applied rotation angles from header
            # ---------------------------------------
            try:
                variostr = variostr.remove_flagged()
                print("Flagged records of variodata have been removed")
            except:
                print("Flagging of variodata failed")
            # ---------------------------------------
            # 1.6 drop flagged data
            # ---------------------------------------
        except:
            print("absoluteAnalysis: reading variometer data failed")
            variostr = DataStream()
        if (len(variostr) > 3 and not np.isnan(variostr.mean('time'))) or len(variostr.ndarray[0]) > 0: # can contain ([], 'File not specified')
            if offset:
                variostr = variostr.offset(offset)
            if not alpha:
                valalpha = 0.0
            else:
                valalpha = float(alpha)
                print ("Rotating vector with manually provided alpha", valalpha)
            if not beta:
                valbeta = 0.0
            else:
                valbeta = float(beta)
                print ("Rotating vector with manually provided beta", valbeta)
            variostr =variostr.rotation(alpha=valalpha, beta=valbeta)
            vafunc = variostr.interpol(['x','y','z'])
            if vafunc[0] == {}:
                print("absoluteAnalysis: check variation data -- data seems to be invalid")
                variofound = False
        else:
            print("absoluteAnalysis: no variometer data available")
            variofound = False
        # ---------------------------------------
        # 1.6 manually provided rotation and offsets applied, interpolated
        # ---------------------------------------

        # b) Load Scalardata
        print("-----------------")
        try:
            if not isinstance(scalardata, list):
                # Scalar information from DB can be a comma separated string -> convert to list
                # or directly be a list
                scalardbtest = scalardata.split(',')
            else:
                scalardbtest = scalardata

            if len(scalardbtest) == 2:
                import magpy.database as dbase
                scalarstr = dbase.readDB(scalardbtest[0],scalardbtest[1],starttime=date,endtime=date+timedelta(days=1))
            else:
                scalarmod = checkURL(scalardata, date)
                scalarstr = read(scalarmod,starttime=date,endtime=date+timedelta(days=1))
            if not scalarstr.header.get('SensorID') == '':
                 scalarid = scalarstr.header.get('SensorID')
            # Check for the presence of f or df
            fcol = KEYLIST.index('f')
            dfcol = KEYLIST.index('df')
            if not len(scalarstr.ndarray[fcol]) > 0 and not len(scalarstr.ndarray[dfcol]) > 0:
                print ("absoluteAnalysis: No F data found in file")
                pass
            elif not len(scalarstr.ndarray[fcol]) > 0:
                scalarstr = scalarstr.calc_f()
            else:
                pass
            print("Length of Scalardata {}: {}".format(scalardbtest[-1],scalarstr.length()[0]))
            print (scalarstr.header.get('SensorID') , scalarstr.header.get('DataID'))
            # ---------------------------------------
            # 2.1 scalar data loaded
            # ---------------------------------------
            if db and not skipscalardb:
                try:
                    scflaglist = dbase.db2flaglist(db,scalarstr.header.get('SensorID'))
                    scalarstr = scalarstr.flag(scflaglist)
                except:
                    print("Failed to obtain flagging information from data base")
                try:
                    print("Now getting header information")
                    scalarstr.header = dbase.dbfields2dict(db,scalarstr.header.get('SensorID')+'_0001')
                except:
                    print("Failed to obtain header information from data base")
                try:
                    print("Applying delta values from database for {}".format(scalarstr.header.get('SensorID')))
                    scalarstr = dbase.applyDeltas(db,scalarstr)
                    if not deltaF == 0:
                        print (" ------------  IMPORTANT ----------------")
                        print (" Both, deltaF from DB and the provided delta F {b}".format(b=deltaF))
                        print (" will be applied.")
                except:
                    print("Applying delta values failed")
            # ---------------------------------------
            # 2.2 If DB: loaded DB flags, applied header and Deltas from DB
            # ---------------------------------------
            try:
                scalarstr = scalarstr.remove_flagged()
                print("Flagged records of scalardata have been removed")
            except:
                print("Flagging of scalardata failed")
            # ---------------------------------------
            # 2.3 removed flagged data
            # ---------------------------------------
        except:
            print("absoluteAnalysis: reading scalar data from file failed")
            scalarstr = DataStream()
        if (len(scalarstr) > 3 and not np.isnan(scalarstr.mean('time'))) or len(scalarstr.ndarray[0]) > 0: # Because scalarstr can contain ([], 'File not specified')
            scfunc = scalarstr.interpol(['f'])
            if scfunc[0] == {}:
                print("absoluteAnalysis: check scalar data -- f data seems to be invalid")
                scalarfound = False
        else:
            print("absoluteAnalysis: no external scalar data provided")
            scalarfound = False

        # c) get absolute data
        abslist = []
        if readfile:
            datestr = datetime.strftime(date,"%Y-%m-%d")
            datestr2 = datetime.strftime(date,"%Y%m%d") # autodif
            print("-----------------")
            print(datestr, abstype)
            if abstype == 'autodif':
                difiles = [di for di in filelist if datestr2 in di]
            else:
                difiles = [di for di in filelist if datestr in di]

            if not len(difiles) > 0:
                # could not find dates in filenames
                difiles = [di for di in filelist]

            #print ("Here", difiles, filelist)

            if len(difiles) > 0:
                for elem in difiles:
                    #if not deltaF:
                    #    deltaF = 0.0
                    absst = absRead(elem,azimuth=azimuth,pier=pier,output='DIListStruct')
                    try:
                        if not len(absst) > 1: # Manual
                            stream = absst[0].getAbsDIStruct()
                            abslist.append(absst)
                            if db and dbadd:
                                dbase.diline2db(db, absst,mode='insert',tablename='DIDATA_'+stationid)
                        else: # AutoDIF
                            for a in absst:
                                stream = a.getAbsDIStruct()
                                abslist.append(a)
                            if db and dbadd:
                                dbase.diline2db(db, absst,mode='insert',tablename='DIDATA_'+stationid)
                        #print "absoluteAnalysis: Successful analyse of %s" % elem
                        successlist.append(elem)
                    except:
                        print("absoluteAnalysis: Failed to analyse %s - problem of filestructure" % elem)
                        failinglist.append(elem)
                        # TODO Drop that line from filelist
                        pass
        elif absstruct:
            abslist = [el for el in absdata if num2date(np.nanmean(el.time)).replace(tzinfo=None).date() == date.date()]
        else:
            #get list from database
            startd = datetime.strftime(date,"%Y-%m-%d")
            endd = datetime.strftime(date+timedelta(days=1),"%Y-%m-%d")
            if not stationid or stationid == '':
                stationid = absdata.split('_')[1]

            sql = "Pier='%s'" % pier
            tablename = absdata

            try:
                abslist = dbase.db2diline(db,starttime=startd,endtime=endd,sql=sql,tablename=tablename)
            except:
                print("absoluteAnalysis: Problems when reading from database")

        for absst in abslist:
            print("-----------------")
            try:
                stream = absst[0].getAbsDIStruct()
                filepier = absst[0].pier
                if not pier == filepier:
                    loggerabs.info(" -- piers in data file(s) and filenames are different - using file content")
                    pier = filepier
            except:
                stream = absst.getAbsDIStruct()

            if stream[0].person == 'AutoDIF':
                abstype = 'autodif'

            if azimuth:
                for i in range(len(stream.container)):
                    stream[i].expectedmire = azimuth

            print("Analyzing %s measurement from %s" % (abstype,datetime.strftime(date,"%Y-%m-%d")))
            # if usestep not given and AutoDIF measurement found
            #print ("Identified pier in file:", stream[0])
            try:
                streamtime = datetime.strftime(num2date(stream[0].time).replace(tzinfo=None),"%Y-%m-%d")
            except:
                print (" absoluteAnalysis: Could not extract an appropriate date from data source")
                streamtime = "2233-03-22"
            if streamtime == datetime.strftime(date,"%Y-%m-%d"):
                if debug:
                    print (" absoluteAnalysis: Times in stream are fine")
            else:
                continue

            if stream[0].person == 'AutoDIF' and not usestep:
                usestep = 2

            if stream[0].person == 'AutoDIF' and not azimuth:
                print("absoluteAnalysis: AUTODIF but no azimuth provided --- this will not work")

            if variofound:
                valuetest = stream._check_coverage(variostr)
                if valuetest:
                    stream = stream._insert_function_values(vafunc,funckeys=['x','y','z'],debug=debug)
                else:
                    print("Warning! Variation data missing at DI time range")
                #print ("stream looks like:", stream)
                #stream = stream._insert_function_values(vafunc)
            if scalarfound:
                valuetest = stream._check_coverage(scalarstr,keys=['f'])
                if valuetest:
                    stream = stream._insert_function_values(scfunc,funckeys=['f'],offset=deltaF,debug=debug)
                else:
                    print ("Warning! Scalar data missing at DI time range")
            try:
                # get delta D and delta I values here
                if not deltaD and db:
                    try:
                        val= dbselect(db,'DeltaDictionary','PIERS','PierID like "{}"'.format(pier))[0]
                        try:
                            dic = string2dict(val,typ='dictionary')
                            res = dicgetlast(dic,pier='A2',element='deltaD,deltaI,deltaF')
                            deltaD = float(res.get('deltaD','0.00001'))
                        except:
                            deltainputs = val.split(',')
                            lastval = deltainputs[-1]
                            deltaD = float(lastval.split('_')[2])
                        print ("Obtained deltaD from database")
                    except:
                        deltaD = 0.0
                if not deltaI and db:
                    try:
                        val= dbselect(db,'DeltaDictionary','PIERS','PierID like "{}"'.format(pier))[0]
                        try:
                            dic = string2dict(val,typ='dictionary')
                            res = dicgetlast(dic,pier='A2',element='deltaD,deltaI,deltaF')
                            deltaI = float(res.get('deltaI','0.00001'))
                        except:
                            deltainputs = val.split(',')
                            lastval = deltainputs[-1]
                            deltaI = float(lastval.split('_')[3])
                        print ("Obtained deltaI from database")
                    except:
                        deltaI = 0.0

                #print ("Running calc:", usestep, annualmeans, deltaD, deltaI, meantime, scalevalue)
                print ("Provided pier differences:")
                print (" delta F for continuous scalar data: {}".format(deltaF))
                print (" delta D: %s, delta I: %s" % (str(deltaD),str(deltaI)))

                result = stream.calcabsolutes(usestep=usestep,annualmeans=annualmeans,printresults=True,debugmode=debug,deltaD=deltaD,deltaI=deltaI,meantime=meantime,scalevalue=scalevalue,variometerorientation=variometerorientation,residualsign=residualsign)
                #print("%s with delta F of %s nT" % (result.str4,str(deltaF)))
                #print("Delta D: %s, delta I: %s" % (str(deltaD),str(deltaI)))
                if not deltaF == 0:
                    result.str4 = result.str4 + "_" + str(deltaF)
                #print("absolutes", valalpha, alpha,result.str4)
                if valalpha != 0 or alpha:
                    #print ("absolute alpha", valalpha,alpha)
                    alphavaluetobeadded = 0
                    if not valalpha == 0:
                        alphavaluetobeadded = valalpha
                    elif not alpha == None:
                        alphavaluetobeadded = alpha
                    if not result.str4 == '':
                        result.str4 = result.str4 + ","
                    result.str4 += "alpha_" + str(alphavaluetobeadded)
                #print("absolutes", result.str4, valbeta, beta)
                if valbeta != 0 or beta:
                    #print ("absolute beta", valbeta,beta)
                    betavaluetobeadded = 0
                    if not valbeta == 0:
                        betavaluetobeadded = valbeta
                    elif not beta == 0:
                        betavaluetobeadded = beta
                    if not result.str4 == '':
                        result.str4 = result.str4 + ","
                    result.str4 += "beta_" + str(betavaluetobeadded)
                #print("absolutes", result.str4)
            except:
                result = LineStruct()
            dataok = True
            if expD:
                if not float(expD)-float(expT) < result.y < float(expD)+float(expT):
                    try:
                        #print ("expD", expD, expT, stream[0], stream[0].time)
                        test = datetime.strftime(num2date(stream[0].time),'%Y-%m-%d_%H-%M-%S')
                        xl = [ el for el in difiles if test in el]
                        if len(xl) > 0:
                            failinglist.append(xl[0])
                        print("absoluteAnalysis: Failed to analyse {} - threshold for acceptable angular offset in Dec exceeded".format(test))
                        dataok = False
                    except:
                        print("absoluteAnalysis: checking expD - Value error likely while determining time - failed analysis")
                        dataok = False
            if expI and dataok:
                if not float(expI)-float(expT) < result.x < float(expI)+float(expT):
                    try:
                        test = datetime.strftime(num2date(stream[0].time),'%Y-%m-%d_%H-%M-%S')
                        xl = [ el for el in difiles if test in el]
                        if len(xl) > 0:
                            failinglist.append(xl[0])
                        print("absoluteAnalysis: Failed to analyse {} - threshold for acceptable angular offset in Inc exceeded".format(test))
                        dataok = False
                    except:
                        print("absoluteAnalysis: checking expI - Value error likely while determining time - failed analysis")
                        dataok = False
            if dataok:
                resultstream.add(result)

    # ####################################
    # 3. Format output
    # ####################################

    # 3.0 Convert result to ndarray and dx,dy,dz to XYZ in nT
    #     --- This is important for baseline correction as all variometer provided components in nT

    # cleanup resultsstream:
    # replace all 999999.99 and -inf with NaN
    resultstream = resultstream.linestruct2ndarray()
    for idx, elem in enumerate(resultstream.ndarray):
        if KEYLIST[idx] in NUMKEYLIST:
            resultstream.ndarray[idx] = np.where(resultstream.ndarray[idx].astype(float)==999999.99,NaN,resultstream.ndarray[idx])
            resultstream.ndarray[idx] = np.where(np.isinf(resultstream.ndarray[idx].astype(float)),NaN,resultstream.ndarray[idx])

    # Add deltaF to resultsstream for all Fext:  if nan then df == deltaF else df = df+deltaF,
    posF = KEYLIST.index('str4')
    posdf = KEYLIST.index('df')

    for idx,elem in enumerate(resultstream.ndarray[posF]):
        #print elem
        #if not deltaF:
        #    deltaF = 0.0
        if not deltaF == 0 and elem.startswith('Fext'):
            try:
                resultstream.ndarray[posdf][idx] = deltaF
            except:
                array = [np.nan]*len(resultstream.ndarray[0])
                array[idx] = deltaF
                resultstream.ndarray[posdf] = np.asarray(array)
        elif not deltaF == 0 and elem.startswith('Fabs'):
            try:
                resultstream.ndarray[posdf][idx] = float(resultstream.ndarray[posdf][idx])+float(deltaF)
            except:
                array = [np.nan]*len(resultstream.ndarray[0])
                array[idx] = deltaF
                resultstream.ndarray[posdf] = np.asarray(array)

    #print "After", resultstream.ndarray[posdf]
    #resultstream = resultstream.hdz2xyz(keys=['dx','dy','dz'])

    if not stationid and varioid == scalarid:
        stationid = varioid

    # 3.1 Header information
    # --------------------
    di_version = '1.1'
    resultstream.header['StationID'] = stationid
    resultstream.header['DataPier'] = pier
    resultstream.header['DataFormat'] = 'MagPyDI'
    resultstream.header['DataType'] = "{}{}".format('MagPyDI',di_version)
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
    if variometerorientation in ["XYZ","xzy"]:
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
    #resultstream.header['SensorID'] = 'BLV'
    #print ("Vario:", varioid)
    #print ("Scalar:", scalarid)
    #print ("Pier:", pier)
    resultstream.header['DataID'] = 'BLV_{}_{}_{}'.format(varioid,scalarid,pier)
    resultstream.header['SensorID'] = 'BLV_{}_{}_{}'.format(varioid,scalarid,pier)

    #print "Files for archive:"
    #print "---------------------------"
    archivelist = [x for x in successlist if x not in failinglist]
    print("------------------------------------------------------")
    print("Failed files:")
    print("------------------------------------------------------")
    if len(failinglist) > 0:
        for el in failinglist:
            print(el)
    else:
        print("None")
    print("------------------------------------------------------")


    # 3.2 Eventually archive the data
    # --------------------
    # XXX possible issues: direct ftp reading or url reading
    if movetoarchive:
        if readfile:
            for fi in archivelist:
                if not "://" in fi:
                    src = fi
                    fname = os.path.split(src)[1]
                    dst = os.path.join(movetoarchive,fname)
                    shutil.move(src,dst)
                else:
                    fname = fi.split('/')[-1]
                    suffix = fname.split('.')[-1]
                    passwdtyp = fi.split(':')
                    typus = passwdtyp[0]
                    port = 21
                    passwd = passwdtyp[2].split('@')[0]
                    restpath = passwdtyp[2].split('@')[1]
                    myproxy = restpath.split('/')[0]
                    ftppath = restpath.split('/')[1]
                    login = passwdtyp[1].split('//')[1]
                    dst = os.path.join(archivepath,fname)
                    fh = NamedTemporaryFile(suffix=suffix,delete=False)
                    print("Fi: ", fi)
                    fh.write(urllib2.urlopen(fi).read())
                    fh.close()
                    shutil.move(fh.name,dst)
                    if (typus == 'ftp'):
                        ftpremove (ftppath=ftppath, filestr=fname, myproxy=myproxy, port=port, login=login, passwd=passwd)

    resultstream = resultstream.sorting()

    #print "Finished", resultstream, resultstream.ndarray

    # Apply correct format to resultsstream
    array = [[] for el in KEYLIST]
    for idx,el in enumerate(resultstream.ndarray):
        if KEYLIST[idx] in NUMKEYLIST or KEYLIST[idx] == 'time':
            array[idx] = np.asarray(el).astype(float)
        else:
            array[idx] = np.asarray(el)
    resultstream.ndarray = np.asarray(array,dtype=object)

    return resultstream


def getAbsFilesFTP(**kwargs):
    """
    Obtain files from the selected directory
    Requires an analysis and an archive directory for treatment of downloaded absolute files
    Analysis directory:
    - contains the newly downloaded files. These files are then analyzed in in case of success transfered to the Archive directory.
    Archive directory:
    - contains successfully analyzed data (requires on errors and variometer correction)
    - downloaded data is checked, whether files are already exist in the archive directory. If true, they are deleted from the ftp server
    Keywords:
    localpath
    ftppath
    filestr(ing)
    myproxy
    port
    login
    passwd
    logfile
    archivepath
    """
    localpath = kwargs.get('localpath')
    ftppath = kwargs.get('ftppath')
    filestr = kwargs.get('filestr')
    myproxy = kwargs.get('myproxy')
    port = kwargs.get('port')
    login = kwargs.get('login')
    passwd = kwargs.get('passwd')
    logfile = kwargs.get('logfile')
    analysispath = kwargs.get('analysispath')
    archivepath = kwargs.get('archivepath')
    absidentifier = kwargs.get('absidentifier') # Part of filename, which defines absolute files

    if not absidentifier:
        absidentifier = '*AbsoluteMeas.txt'

    filelist = []

    loggerabs.info(" -- Starting downloading Absolute files from %s" % ftppath)

    # -- Checking whether new data is available
    ftplist = ftpdirlist (localpath=localpath, ftppath=ftppath, filestr=filestr, myproxy=myproxy, port=port, login=login, passwd=passwd, logfile=logfile)
    if len(ftplist) == 0:
        loggerabs.info(" ---- AbsDownload: no new data on ftp directory")
    # -- get local list
    for infile in iglob(os.path.join(archivepath,absidentifier)):
        filelist.append(infile)

    # -- Download files to Analysis folder
    for f in ftplist:
        loggerabs.info(" ---- Now getting %s" % f)
        ftpget (localpath=analysispath, ftppath=ftppath, filestr=f, myproxy=myproxy, port=port, login=login, passwd=passwd, logfile=logfile)

    loggerabs.info(" -- Download procedure finished at %s" % ftppath)

    return ftpfilelist, localfilelist

def removeAbsFilesFTP(**kwargs):
    """
    Remove files from the selected directory
    Requires an analysis and an archive directory for treatment of downloaded absolute files
    Analysis directory:
    - contains the newly downloaded files. These files are then analyzed in in case of success transfered to the Archive directory.
    Archive directory:
    - contains successfully analyzed data (requires on errors and variometer correction)
    - downloaded data is checked, whether files are already exist in the archive directory. If true, they are deleted from the ftp server
    Keywords:
    ftppath
    filestr(ing)
    myproxy
    port
    login
    passwd
    ftpfilelist
    localfilelist
    """
    ftppath = kwargs.get('ftppath')
    filestr = kwargs.get('filestr')
    myproxy = kwargs.get('myproxy')
    port = kwargs.get('port')
    login = kwargs.get('login')
    passwd = kwargs.get('passwd')
    ftpfilelist = kwargs.get('ftpfilelist')
    localfilelist = kwargs.get('localfilelist')

    loggerabs.info(" -- Starting removing already successfully analyzed files from %s" % ftppath)

    doubles = list(set(ftpfilelist) & set(localfilelist))
    print(doubles)

    for f in doubles:
        ftpremove (ftppath=ftppath, filestr=f, myproxy=myproxy, port=port, login=login, passwd=passwd)

    loggerabs.info(" -- Removing procedure finished at %s" % ftppath)


# ##################
# ------------------
# Main program
# ------------------
# ##################


if __name__ == '__main__':
    print("Starting a test of the Absolutes program:")
    #msg = PyMagLog()
    ### TODO: To be implemented
