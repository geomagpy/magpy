#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 22 11:56:39 2019

@author: Niko Kompein

Freq. Filter

Filters given equidistantly sampled series's "A" with sampling interval "dt"
by frequency range selection.

"""

import numpy as np
import sys
#sys.stdout.flush()
import scipy.signal as sg
#import scipy.fftpack as fft # replaced cause it old and obsolet by scipy.fft
import scipy.fft as fft
import scipy.interpolate as ipol
from scipy.optimize import curve_fit, minimize_scalar
from itertools import chain
import time
from statistics import mode
from scipy.signal.windows import dpss

debug = False # Print every single output

import matplotlib.pyplot as plt
if( debug):
    print( 'pyplot imported as plt for debugging purposes')

class FrqFilter:
    """
    
    FrqFilter, V1.8
    
    
    -- Added a subfunction self.__SlepProd__( self.NumOfSlep, self.data) to derive
    the slepian tapered spectra for self.fdataMag
    
    -- Added a subfunction self.EigWeigh( ...) to derive the weights for an
    iterative approach to derive the optimal amount of slepian taper windows
    according to "Thomson’s Multitaper Method Revisited Santhosh Karnik, 
    Justin Romberg, Mark A. Davenport arXiv:2103.11586v1 [eess.SP] 22 Mar 2021
    March 23, 2021"
    
    -- Added a subfunction self.EigSpec( ...) to derive the weights for an
    iterative approach to derive the optimal amount of slepian taper windows
    according to "Thomson’s Multitaper Method Revisited Santhosh Karnik, 
    Justin Romberg, Mark A. Davenport arXiv:2103.11586v1 [eess.SP] 22 Mar 2021
    March 23, 2021"
    
    -- Added a subfunction self.ThomsonMultitaperAdj( ...) to derive the weights 
    for an iterative approach to derive the optimal amount of slepian taper windows
    according to "Thomson’s Multitaper Method Revisited Santhosh Karnik, 
    Justin Romberg, Mark A. Davenport arXiv:2103.11586v1 [eess.SP] 22 Mar 2021
    March 23, 2021" calling functions self.EigSpec and self.EigWeigh
    
    -- corrected some scaling error in the slepian taper averaging method due
    to wrong usage of Amplitude power spectral density to average instead of correctly
    using power spectral density and take the square-root of that values.
    Also missed the scaling of PSD (power spectral density) by 
    self.slepScale = 2.0* self.fnyq/ np.pi
    
    --- under construction: added a PCARotate for frequency spectrum in order
    to find subharmonics in a spectrum an trying to remove it by PCA rotation
    
    
    
    
    FrqFilter, V1.7
    
    
    Corrected some errors in the selection of Despike for axis == 0 and 
    axis == 1 as well as added a selection subroutine to select all samples
    which happen to be minima or maxima, regardless of amplitude.
    Changed the Despike routine to derive a despiked timeseries from which a
    lower and upper bound is derived along the individual samples based on the
    inner quartile range by q3, q1 = np.percentile( DiffData, self.percvals) where
    self.percvals is a set bound with a list defined as self.percvals = [75,25]...
    
    
    ...
    FrqFilter, V1.5
    
    
    Implemented lbub-filter type ( lower boundary upper boundary filter
    with exponential decay on both sides defineable through lower bound frq lb
    , upper bound frq ub and damping decrement de). Frequencies and Periods are
    supported but have to mentioned by PeriodAxis = True in the calling process
    .
    
    Implemented field-augmentation on both axis ... error was present for 
    augmentation on axis == 1
    
    Implemented Despike for axis == 0 and axis == 1 as well as 
    __MySlepianApproach__ which now also supports axis == 1
    
    
    ...
    FrqFilter, V1.4
    
    
    Preparation for GitHub upload
    Change from scale = self.fmin to scale = np.sqrt( self.fmin* self.dt**2.0)
    init scale as self.AmplitudeScale
    on 13.10.2022.
    
    
    Correct linear interpolation inaccuracy by replacing
    
    self.t = np.arange( 0, self.len* self.dt, self.dt)
    
    with self.t = np.arange( 0, self.len)* self.dt
    
    ...
    FrqFilter, V1.3
    
    
    Corrected a scaling bug for the absolute valus of the spectra ...
    
    Change from scale = self.fmin to scale = np.sqrt( self.fmin* self.dt**2.0)
    on 16.2.2022.
    
    ...
    FrqFilter, V1.2
    
    
    Reading 2D numpy.ndarray of "data" with equidistantly sampled timestamps in
    zerotime format and derives FFT of "data" ( "fdata"). Maybe some 
    sophisticated field-augmentation to suppress leakaging by using option 
    aug = True is applied. Anyway, aliasing may still be present in the "data".
    
    Suppression of leakage is based on an idea by Ramon Egli, ZAMG-Austria.
    
    
    
    Addtional an average noise level for spectra can be calculated using
    GetNoiseLevel() and the filtered spectra as well as the filtered timeseries'
    using frqfilter()
    
    ...
    
    Attributes
    ----------
    
    keywords:
        data:
            numpy.ndarray
            A numpy ndarray with A.ndim<=2
            
            
            
        axis:
            int
            Axis for fft calculation. Has to be [ 0 | 1] for ndim==2 and can be
            neglected for ndim==1  --- under construction --- only axis = 0
            valid so far
            
            
            
        dt:
            float
            nominal sampling of series "A"
            
            
            
        {Optional:
        PeriodAxis:
            Bool Value
            True:
                Periods instead of frequency values are returned for "faxis"
            False:
                Frequency values are returned for "faxis" [default]
            
            
            
        aug:
            Bool-Value
            True:
                Sofisticated field augmentation is applied before FFT 
                calcluation. The timeseries' a(t) with length N are mirrored at
                the start and end. By doing so the original timeseries is 
                augmented to M = 3*N. Cutting out the central part with length
                L = 2**n > N one gets timeseries' b(t). From the new start and 
                end points from b(t) the average values b_avg of b(0) and b(L) 
                are derived. From a(0) to b(0) there is a detrend applied to 
                change b(0) to b_avg whereas a(0) remains the same. From a(N) 
                to b(L) there is a detrend applied to change b(L) to b_avg 
                whereas a(N) remains the same. Finally for both timeranges b(0)
                to a(0) and a(N) to b(L) the first and second half of a parzen 
                window is applied respectively. TAKE CARE: input data should be
                WITHOUT offset, otherwise unwanted effects may occur.
                
            False:
                Standard FFT zero padding is used to get 
                np.mod( np.shape( data), 2) == 0
            
            
            
        lbub | exp:
            lbub:
                Lower boundary and upper boundary frequency will be 
                given as tuple ( lb, ub, de) with damping decrement de.
                damping decrement is influencing the slope at the edges of
                the window. The slope is derived as explained in "exp"
                option ( see below). Decreasing and increasing slopes are
                derived through that functions. Between lb and ub the value
                of the window function is 1.0.
            exp:
                Center frequency and damping decremnt will be given 
                for a doublesided exponential filter function with 
                tuple of center frequency and damping 
                decrement ( ce, de). de influences the exponential filter
                as follows
                window:
                    D = np.exp( de)
                    tauval = float( N)/2.0 * 8.69/ D
                    from:
                        https://en.wikipedia.org/wiki/Window_function
                        from Exponential or Poisson window
                    N...length of window
                    tau...damping decrement in dB
                    center...index of maxval of window
                    window = scipy.signal.exponential(
                            N, center = self.ctrind, tau = tauval
                            , sym = False)
                    
                    
                    
        kind: {has to be specified when lbub or cede option is used}
            block:
                Blocking frequency range between lb and ub or inside 
                the doublesided expontential filter with inverse 
                effect of damping decrement de. Filter window calculates by
                window = "window of some type"
                window = 1.0 - window
            pass:
                Passing frequency range between lb and ub or inside the
                doublesided expontential filter with damping 
                decrement de.
                
                
                
        despike:
            boolean value
            activate despiking before field augmentation, default is
            "False". Can be called separately too by:
                FrqCont = FrqFilter( data = data, axis = myaxis, dt = dt, 
                                    rplval = 'linear', lvl = mylvl, 
                                    despike = True, despikeperc = 0.01, 
                                    percvals = [75, 25])
                despikedData, chgind = FrqCont.DeSpike()
            whereas:
                despikedData:   ... despiked ndarray
                chgind:         ... indices of changed samples per column/
                                    row
            Options keywords:
                rplval:...type of replacement as string or float:
                    'mean', 'linear', 'cubic', float(arg)
                    mean:
                        replaces datasample to be replaced with weighted mean 
                        of nearest good samples
                    linear:
                        scipy.interpolate.interp1d 'linear' interpolation with
                        'bound_errors'=False is used to replace datasamples
                    cubic:
                        scipy.interpolate.interp1d 'cubic' interpolation with
                        'bound_errors'=False is used to replace datasamples
                        
                        
                        
            despikeperc:
                float
                Define up to which percentage a timeseries will be 
                despiked. Values between 0.0 and 1.0 are allowed.
                0.0 means no value will be despiked...equivalent with
                despike = False
                1.0 all found spikes according which are bigger than 
                defined "lvl" will be replaced by method "rplval".
            lvl:
                a list of values of per component defined levels to be 
                exceeded that sample will be marked as bad sample and 
                therefore be replaced by rplval
            percvals:
                two element list of percentage values for the bounds of 
                the inner quartile range to be derived from the first 
                derivative of self.data by using those element which 
                are obviously maxima or minima (badind) with 
                q3, q1 = np.percentile( DiffData[badind], self.percvals)
                default is self.percvals = [75, 25]
                .
                Therefore first percentage value needs to be higher than 
                last.
        }
        
        
        
    posind:
        Integer array
        Indices of elements with positive frequencies bigger or equal than 
        fmin. If PeriodAxis == True than indices of elements with positive 
        periods bigger or equal than 1.0/fnyq (Nyquist-frequency) are returned
        
        
        
    negind:
        Integer array
        Indices of elements with negative frequencies smaller or equal than 
        -fmin. If PeriodAxis == True than indices of elements with negative 
        periods smaller or equal than -1.0/fnyq (Nyquist-frequency) are 
        returned.
        
        
        
    fmin:
        Float value
        Smallest positive frequency for used sampling-rate and length of 
        timeseries
        
        
        
    fnyq:
        Float value
        Biggest positive frequency according to Nyquist frequency 
        (fnyq = 1.0/ 2.0/ dt)
    
    
    
    Methods
    -------
        
        fdata():
            Derives the FFT for "data" and returns "fdata". Magnitudes are
            scaled by np.sqrt( fmin).
        
        
        
        FIELDAUGMENT( zerotime, data):
            zerotime:
                numpy.ndarray of timestamps
                
                
                
            data:
                numpy.ndarray (max 2D) of datavalues with N, M = np.shape(data)
            Calculates an artificial field augmented/ field extended timeseries
            which is shorter than 3* maxdim 
            ( maxdim = np.max( [N, M], axis = self.axis)). 
            Augmented length is equal to augleng:
                int( power(2.0, np.log2( np.float(np.ceil( 1.5*maxdim)))))
        
        
        
        fdataMag():
            Derives the magnitude of FFT for "data" and returns slepian 
            windowed "fdataMag". Maybe removes noise in spectrum a bit. 
            Contains only magnitude values. No phase information. Magnitudes 
            are scaled by np.sqrt( fmin).
        
        
        
        fdataAng():
            Derives the angles of FFT for "data" and returns slepian
            windowed "fdataAng". Maybe removes noise in spectrum a bit. 
            Contains only angle values [-np.pi, np.pi]. No magnitude 
            information.
        
        
        
        AnaSig(data = Data, dt = dt, axis = axis):
            Derives the analytical-signals ( complex values for each 
            timeseries) of the given timeseries' using scipy.signal.hilbert
            and self.FIELDAUGMENTATION. !!! Currently only supports calculation
            along axis = 1. !!!
        
        
        
        frqfilter():
            Derives the filtered spectrum as well as the filtered timeseries
            and returns the filtered "faxis", "fdata", "filter" and "filtdata".
            faxis, fdata both are having the appropriate length/ shape
            according to FFT rules. filter on the contrary can be different in
            length if field augmentation is True. filtdata is the filtered 
            data with the same length as the original data used.
        
        
        
        GetNoiseLevel(OPTIONAL:{data = fdata,  perc = perc}):
            Derives a float value representing the average noise lvl according 
            to parameter "perc"(entage) of "data" series length. Default value
            is 0.05 for 5% of length of dataseries beeing put into the
            processing. Without parameters it calculates average noise level
            for the whole dataset in "data" using a slepian windowed FFT
            magnitude spectrum approach.
        
        DeSpike(OPTIONAL:{data = fdata,  lvl = lvl}):
            Removes spikes Nans and Infs from data along axis according to
            exceeding of lvl
    
    
    
    Returns
    -------
        
        faxis: 
            numpy.array
            Array of axis values for given datetime.objects. Can be returned as
            frequency or period values. In case of period values the ordering
            of the values in fdata/ fdataMag/ fdataAng are reversed according
            to ascending faxis values
        
        fdata:
            numpy.array
            Array of complex-spectrum-datavalues equidistantly spaced for 
            every column in "sensortype"-data
        
        fdataMag:
            numpy.array
            Array of magnitude-values equidistantly spaced for every column 
            in "sensortype"-data weighed with slepian windows
        
        fdataAng:
            numpy.array
            Array of angle-values( [-np.pi, np.pi]) equidistantly spaced for 
            every column in "sensortype"-data weighed with slepian windows
        
        AnaSig:  --- under construction ---
            numpy.arrays
            Two arrays ( MagAnaSig, PhAnaSig) containing the 
            analytical-signal's magnitudes and phase-angles
        
        filter:
            numpy.array
            Array of window for filtering for twosided FFT (neg. and pos. 
            frequencies)
        
        filtdata:
            numpy.array
            Array of filtered timeseries'
        
        GetNoiseLevel:
            float
            Float value representing the average noise level
        
        DeSpike:
            numpy.array
            Returns a numpy array of those samples given. Samples exceeding
            a given noise level above or below a filtered timeseries of the
            original are dropped and replaced according to rplval
        
    
    
    Examples
    -------
        
        from frqfilter import FrqFilter
        
        FrqCont = FrqFilter(data = data, axis = 0, 
                            dt = 10.0, aug = True)
        
        faxis, fdata = FrqCont.fdata()
        
        
        
        FrqCont = FrqFilter(data = data, axis = 0, 
                            dt = dt, PeriodAxis = False, aug = True)
        
        faxis, fdata = FrqCont.fdataMag()
        
        
        
        FrqCont = FrqFilter(data = data, PeriodAxis = True
                            , aug = False)
        
        faxis, fdata = FrqCont.fdataAng()
        
        
        
        FrqCont = FrqFilter(data = data, axis = 0, 
                            dt = 10.0, aug = True, 
                            lbub = ( lb, ub, de), kind = 'pass', 
                            rplval = 'linear')
        
        faxis, fdata, filter, filtdata = FrqCont.frqfilter()
        
        
        
        FrqCont = FrqFilter(data = data, axis = 0, 
                            dt = 10.0, aug = True, 
                            exp = ( ce, de), kind = 'block', 
                            rplval = 'linear')
        
        faxis, fdata, filter, filtdata = FrqCont.frqfilter()
        
        
        
        NoiseLvl = FrqCont.GetNoiseLevel(data = fdata, perc = 0.03)
        
        
        
        FrqCont = FrqFilter( data = data, axis = myaxis, dt = dt, 
                            rplval = 'linear', lvl = mylvl, despike = True, 
                            despikeperc = 0.01, percvals = [75, 25])
        
        despikedData, chgind = FrqCont.DeSpike()
        
        
        
        FrqCont = FrqFilter( data = data, axis = myaxis, dt = dt)
        
        MagAnaSig, PhAnaSig = FrqCont.AnaSig()
    
    
    
    """
    class MyException(Exception):
        """
        raise self.MyException("My Exception test is...")
        """
        pass
    
    
    
    def __init__( self, **kwargs):
        ###############
        # Defaults
        ###############
        self.PeriodAxis = False
        self.axis = 0
        self.dt = 1.0
        self.aug = False
        self.NumOfSlep = 5
        self.DynNumOfSlep = False
        self.debug = debug
        self.despike = False
        self.despikeperc = 1.0
        self.PercMostOffSamplesList = []
        self.percvals = [75, 25]
        self.RemoveSubHarm = False
        
        
        ###############
        for item, value in kwargs.items():
            #if( not debug):
            #    print( 'item is: {}'.format( item))
            if( 'debug' == item):
                self.debug = value
        try:
            for item, value in kwargs.items():
                #if( not debug):
                #    print( 'item is: {}'.format( item))
                if( 'data' == item):
                    """
                    do .....
                    
                    set up data array A
                    """
                    self.data = value
                    if( self.debug):
                        print( 'self.data is: {}'.format( self.data))
                    
                    self.dim = np.ndim( self.data)
                    if( self.debug):
                        print( 'self.dim is: {}'.format( self.dim))
                    
                    self.shape = np.shape( self.data)
                    if( self.debug):
                        print( 'self.shape is: {}'.format( self.shape))
                if( 'axis' == item):
                    """
                    
                    
                    get axis to derive frequency axis from
                    """
                    self.axis = value
                    if( self.debug):
                        print( 'self.axis is: {}'.format( self.axis))
                if( item == 'despike'):
                    """
                    
                    
                    set despiking to True or False
                    """
                    self.despike = value
                    if( not isinstance( self.despike, bool)):
                        raise self.MyException( 'Despike switch not correctly set...')
                    for item, value in kwargs.items():
                        #if( not debug):
                        #    print( 'item is: {}'.format( item))
                        if( item == 'despikeperc'):
                            """
                            
                            setting percentage value of how much datapoints have to valid
                            to apply DeSpike
                            """
                            self.despikeperc = value
                            if( not isinstance( self.despikeperc, float)):
                                raise self.MyException( 'Despikeperc is no float...')
                            if( self.debug):
                                print( 'self.despikeperc in __init__ routine: {}'.format( self.despikeperc))
                                #sys.exit()
                        if( 'percvals' == item):
                            """
                            
                            
                            get the parameters for inner quartile range
                            for Despiking routine
                            """
                            self.percvals = value
                            if( self.debug):
                                print( 'self.percvals is: {}'.format( self.percvals))
            #print( 'np.shape( self.data)', np.shape( self.data))
            #sys.exit()
            #plt.plot( self.data.flatten(), alpha = 0.3, label = 'self.data')
            #plt.plot( np.gradient( self.data, axis = self.axis).flatten(), alpha = 0.3, label = 'gradient of self.data in self.axis column')
            #plt.show()
            #print( 'np.shape( np.gradient( self.data, axis = self.axis))', np.shape( np.gradient( self.data, axis = self.axis)))
            #print( 'np.shape( np.nanstd( np.gradient( self.data, axis = self.axis), axis = self.axis) )', np.shape( np.nanstd( np.gradient( self.data, axis = self.axis), axis = self.axis) ))
            #print( 'np.nanstd( np.gradient( self.data, axis = self.axis), axis = self.axis)', np.nanstd( np.gradient( self.data, axis = self.axis), axis = self.axis))
            #sys.exit()
            self.lvl = 3.0 * np.nanstd( np.gradient( self.data, axis = self.axis), axis = self.axis) 
            self.rplval = 'mean'
            # checking for special DeSpiking procedure parameters
            #############
            for item, value in kwargs.items():
                if( item == 'rplval'):
                    self.rplval = value
                    if( self.debug):
                        print( 'self.rplval is: {}'.format( self.rplval))
                if( item == 'lvl'):
                    self.lvl = value
                    if( self.debug):
                        print( 'self.lvl is: {}'.format( self.lvl))
            #############
                if( self.debug):
                    print( 'item is: {}'.format( item))
                if( 'dt' == item):
                    """
                    
                    derive frequency axis
                    
                    """
                    self.dt = float( value)
                    if( self.debug):
                        print( 'self.dt is: {}'.format( self.dt))
                    self.len = self.shape[self.axis]
                    self.baklen = self.len
                    #self.t = np.arange( 0, float( self.len)* self.dt, self.dt)
                    self.t = np.arange( 0, float( self.len))* self.dt
                    if( self.debug):
                        print( 'self.t is: {}'.format( self.t))
                    self.fnyq = 1.0/ 2.0/ self.dt
                    if( self.debug):
                        print( 'self.fnyq is: {}'.format( self.fnyq))
                    if( self.debug):
                        print( 'self.len is: {}'.format( self.len))
                    self.fmin = 1.0/ float( self.len)/ self.dt
                    if( self.debug):
                        print( 'self.fmin is: {}'.format( self.fmin))
                    self.slepScale = 2.0* self.fnyq#/ np.pi
                    if( self.debug):
                        print( 'self.slepScale is: {}'.format( self.slepScale))
                if( 'PeriodAxis' == item):
                    """
                    
                    Define wether a period axis should be used or a frequency axis
                    
                    """
                    self.PeriodAxis = value
                    if( self.debug):
                        print( 'self.PeriodAxis is: {}'.format( self.PeriodAxis))
                if( 'aug' == item):
                    """
                    
                    
                    get augmentation True or False flag
                    """
                    self.aug = value
                    #ce = value[0] # center frequency
                    #de = value[1] # damping decrement
                    if( self.debug):
                        print( 'self.aug is: {}'.format( self.aug))
                if( 'lbub'  == item):
                    """
                    
                    
                    get lower and upper boundary for filter and damping decrement
                    """
                    self.type = item
                    self.lb = ( value[0])
                    self.ub = ( value[1])
                    self.ce = ( None)
                    self.de = ( value[2])
                    #lb = value[0] # lower boundary frequency
                    #ub = value[1] # upper boundary frequency
                    #de = value[2] # damping decrement
                    if( self.debug):
                        print( 'self.lb, self.ub, self.ce, self.de are: {}, {}, {}, {}'.format( self.lb, self.ub, self.ce, self.de))
                if( 'exp' == item):
                    """
                    
                    
                    get centre frequency for filter and damping decrement
                    """
                    self.type = item
                    self.lb = ( None)
                    self.ub = ( None)
                    self.ce = ( value[0])
                    self.de = ( value[1])
                    #de = value[2] # damping decrement
                    #ce = value[0] # center frequency
                    if( self.debug):
                        print( 'self.lb, self.ub, self.ce, self.de are: {}, {}, {}, {}'.format( self.lb, self.ub, self.ce, self.de))
                if( 'kind' == item):
                    """
                    
                    
                    get passing or blocking behaviour of filter
                    """
                    self.kind = value
                    #ce = value[0] # center frequency
                    #de = value[1] # damping decrement
                    if( self.debug):
                        print( 'self.kind is: {}'.format( self.kind))
                if( 'DynSlep' == item):
                    """
                    
                    
                    get DynNumOfSlep of filter
                    """
                    self.DynNumOfSlep = value
                    #ce = value[0] # center frequency
                    #de = value[1] # damping decrement
                    if( self.debug):
                        print( 'self.DynNumOfSlep is: {}'.format( self.DynNumOfSlep))
                if( 'NumOfSlep' == item):
                    
                    """
                    
                    
                    get NumOfSlep of filter
                    """
                    self.NumOfSlep = value
                    #ce = value[0] # center frequency
                    #de = value[1] # damping decrement
                    if( self.debug):
                        print( 'self.NumOfSlep is: {}'.format( self.NumOfSlep))
                if( 'RemoveSubHarm' == item):
                    """
                    
                    
                    get RemoveSubHarm flag
                    """
                    self.RemoveSubHarm = value
                    #ce = value[0] # center frequency
                    #de = value[1] # damping decrement
                    if( self.debug):
                        print( 'self.RemoveSubHarm is: {}'.format( self.RemoveSubHarm))
            """
            empty list for despiked series to remember which samples where
            replaced by self.rplval
            """
            
            self.newchgind = []
            
            """
            FFT amplitude scaling factor
            """
            
            self.AmplitudeScale = np.sqrt( self.fmin* self.dt**2.0) # will be changed during AUGMENTATION
        except Exception as ex:
            """
            
            
            If everything goes wrong
            """
            print( 'Somethings wrong, exception is: {}'.format( ex))
            raise self.MyException(ex)
            #What to do if invalid parameters are passed
        return
    
    
    
    def crossings_nonzero_all(self, data):
        pos = data > 0
        npos = ~pos
        zerocrossings = ((pos[:-1] & npos[1:]) | (npos[:-1] & pos[1:])).nonzero()[0]
        return len( zerocrossings)
    
    
    
    def __faxis__( self):
        self.fmin = 1.0/ float( self.len)/ self.dt
        self.fnyq = 1.0/ 2.0/ self.dt
        self.faxis = fft.fftshift( fft.fftfreq( self.len, self.dt))
        if( self.debug):
            print( 'self.faxis is: {}'.format( self.faxis))
            print( 'np.max( np.abs( self.faxis)) is: {}'.format( np.max( np.abs( self.faxis))))
            print( 'self.fnyq is: {}'.format( self.fnyq))
        poscheck = np.array( self.faxis >= 0.0, dtype = bool)
        posind = np.argwhere( poscheck).flatten()
        self.faxis[posind] = self.faxis[posind] + self.fmin
        #self.faxis = fft.fftfreq( self.len, self.dt)
        
        if( self.PeriodAxis):
            self.faxis = 1.0/ self.faxis
            fmax = 1.0/ self.fmin
            fmin = 1.0/ self.fnyq
            negfmax = -1.0/ self.fmin
            negfmin = -1.0/ self.fnyq
            #a = self.faxis >= fmin
            #b = self.faxis <= fmax
            #c = self.faxis >= negfmax
            #d = self.faxis <= negfmin
            #self.posind = np.argwhere( a & b).flatten()#[0]
            #self.negind = np.argwhere( c & d).flatten()#[0]
        else:
            fmin = self.fmin
            fmax = self.fnyq
            negfmax = -self.fnyq
            negfmin = -self.fmin
            #self.posind = np.argwhere( self.faxis >= self.fmin).flatten()#[0]
            #self.negind = np.argwhere( self.faxis <= -self.fmin).flatten()#[0]
        if( self.debug):
            print( 'fmin', fmin, 'fmax', fmax, 'negfmax', negfmax, 'negfmin', negfmin)
        a = self.faxis >= fmin
        b = self.faxis <= fmax
        c = self.faxis >= negfmax
        d = self.faxis <= negfmin
        
        self.posind = np.argwhere( a & b).flatten()#[0]
        #self.negind = np.argwhere( c & d).flatten()#[0]
        self.negind = np.argwhere( np.invert( a & b)).flatten()#[0]
        if( self.debug):
            print( 'self.faxis is: {}'.format( self.faxis))
            print( 'np.max( np.abs( self.faxis)) is: {}'.format( np.max( np.abs( self.faxis))))
            print( 'self.fnyq is: {}'.format( self.fnyq))
            print( 'self.faxis, self.posind, self.negind are: {}, {}, {}'.format( self.faxis, self.posind, self.negind))
        if( self.debug):
            print( 'self.faxis is: {}'.format( self.faxis))
            print( 'shapes of self.faxis, self.posind, self.negind are: {}, {}, {}'.format( np.shape( self.faxis), np.shape( self.posind), np.shape( self.negind)))
        #sys.exit()
        return
    
    
    
    def FindPercMostOffSamples(self, **kwargs):
        #self.debug = True
        print('\nCalling FindPercMostOffSamples...')
        
        cpu_float_info = np.finfo(np.longdouble) ### get actual size of smallest
        # float point number of CPU in use.
        if( __name__ == "__main__"):
            self.__init__()
        else:
            pass
        
        
        
        DiffData = np.diff( self.data, axis = self.axis)
        try:
            DiffData = DiffData - np.atleast_2d( np.nanmean( DiffData, axis = self.axis)).T
        except:
            DiffData = DiffData - np.atleast_2d( np.nanmean( DiffData, axis = self.axis))
        
        
        
        SignDfData = np.sign( DiffData)
        if( self.axis == 1):
            SignDfData = np.hstack( ( SignDfData, -SignDfData[:, -1:] ) )
        if( self.axis == 0):
            SignDfData = np.vstack( ( SignDfData, -SignDfData[-1:, :] ) )
        #if( self.axis == 1):
        #    SignDfData = np.hstack( ( -SignDfData[:, 1:1], SignDfData ) )
        #if( self.axis == 0):
        #    SignDfData = np.vstack( ( -SignDfData[:1, :], SignDfData ) )
        
        
        
        if( self.axis == 1):
            DiffData = np.hstack( ( DiffData, DiffData[:, -1:] ) )
            #DiffData = np.hstack( ( DiffData[:, :1], DiffData ) )
        if( self.axis == 0):
            DiffData = np.vstack( ( DiffData, DiffData[-1:, :] ) )
            #DiffData = np.vstack( ( DiffData[:1, :], DiffData ) )
        
        if( self.debug):
            print( 'shape of self.data: {}'.format( np.shape( self.data)))
            print( 'shape of DiffData: {}'.format( np.shape( DiffData)))
            print( 'shape of SignDfData: {}'.format( np.shape( SignDfData)))
        
    
        
        #DiffSignDfData = np.diff( SignDfData, axis = self.axis)
        
        if( self.axis == 1):
            
            DiffSignDfData = SignDfData[:, :-1] + SignDfData[:, 1:]
            #DiffSignDfData = np.hstack( ( DiffSignDfData, DiffSignDfData[:, -1:] ) )
            DiffSignDfData = np.hstack( ( DiffSignDfData[:, :1], DiffSignDfData ) )
            #DiffSignDfData = np.hstack( ( DiffSignDfData[:, :1], DiffSignDfData) )
            
            #DiffDiffData = np.hstack( ( DiffDiffData, DiffDiffData[:, -1:] ) )
            #DiffDiffData = np.hstack( ( DiffDiffData[:, :0], DiffDiffData) )
            
        if( self.axis == 0):
            DiffSignDfData = SignDfData[:-1, :] + SignDfData[1:, :]
            #DiffSignDfData = np.vstack( ( DiffSignDfData, DiffSignDfData[-1:, :] ) )
            DiffSignDfData = np.vstack( ( DiffSignDfData[:1, :], DiffSignDfData ) )
            #DiffSignDfData = np.vstack( ( DiffSignDfData[:1, :], DiffSignDfData) )
            
            #DiffDiffData = np.vstack( ( DiffDiffData, DiffDiffData[-1:, :] ) )
            #DiffDiffData = np.vstack( ( DiffDiffData[:0, :], DiffDiffData) )
        
        
        

        
        if( self.debug):
            print( 'DiffData: {}'.format( DiffData))
            print( 'self.data: {}'.format( self.data))
            
            
            
            
            print( 'shape of self.data: {}'.format( np.shape( self.data)))
            print( 'shape of SignDfData: {}'.format( np.shape( SignDfData)))
            print( 'shape of DiffSignDfData: {}'.format( np.shape( DiffSignDfData)))
            
            
            print(f"Machine Epsilon (eps): {cpu_float_info.eps}")
            print(f"Number of Mantissa Bits: {cpu_float_info.nmant}")
            print(f"Smallest usable positive number (tiny): {cpu_float_info.tiny}")
            
        if( self.axis == 1):
            DiffData, self.data, DiffSignDfData = DiffData.T, self.data.T, DiffSignDfData.T
        for d, od, dd in zip( DiffData.T, self.data.T, DiffSignDfData.T):
            # check each column separately
            bd = np.copy( d)
            bd = np.hstack( ( bd[0], bd, bd[-1]) )
            d = d - np.nanmedian( d)
            MaxDSortedList = []
            MaxDDSortedList = []
            if( self.debug):
                print( 'd is: {}'.format( d))
                
            """
            Condition to be met for finding maxima
            Check where the sum of two consecutive DiffData sample signs sum up
            to zero and therefor a change in trend happens 
            (aka a maximum or a minimum)  --- same indices as self.data                    
            """
            diffcond = ( ( d < bd[:-2]) & ( d < bd[2:])) | ( ( d > bd[:-2]) & ( d > bd[2:]))
            #### introducing numpy array disguised by bd comparision ... so diffcond is already a numpy array
            MaxCond = ( dd == 0) & ( np.abs( d) > 10.0* cpu_float_info.eps) & diffcond #( ( dd < 0 ) | ( dd > 0 ) )
            
            """
            Find the indices to that maxima found with same indices as
            self.data
            """
            MaxInd = np.argwhere( MaxCond).flatten()
            
            """
            Return the maxima values to that indices in DiffData --- NOT the 
            orignal values if 
            
            e.g.
            od = [1,-2,3,-4,5,-6,-7,-8] - aka self.data
            
            then
            
            d =  [-3, 5,-7, 9,-11,-1,-1,-1] - aka DiffData ### artifically 
            extended at the last sample
            
            then
            
            dd = [0, 0, 0, 0, 0, -2, -2, -2] - aka DiffSignDfData ### 
            artifically extended at the first sample and the last sample of the  
            SignDfData variable is artifically multiplied by -1.
            
            now all three are equally long, 
            
            d extended artifically at the last sample by -11, by repetition of 
            last sample
            
            dd extended artifically at first sample
            with the sum derived by sign( -3) + sign( 5)
            """
            try:
                MaxD = np.array( od[ MaxInd ])
            except IndexError as ex:
                print( 'od', od)
                print( 'MaxInd', MaxInd)
                sys.exit()
            
            """
            Return the maxima values to that indices in self.data --- NOT 
            DiffData
            """
            MaxDD = np.array( d[ MaxInd ])
            
            """
            Sort the maxima found in DiffData in decreasing order
            """
            MaxDSortedDInd = np.argsort( np.abs( MaxD))[::-1]
            """
            Sort the maxima found in self.data in decreasing order
            """
            MaxDDSortedDInd = np.argsort( np.abs( MaxDD))[::-1]
            
            if( False):#self.debug):
                plt.close()
                plt.loglog( np.abs( MaxD)[ MaxDSortedDInd], label = 'MaxD sorted decreasing')
                plt.loglog( np.abs( MaxDD)[ MaxDDSortedDInd], label = 'MaxDD sorted decreasing')
                plt.legend( loc = 'best')
                plt.show()
                
                
            """
            Limit the returned amount of found maxima according to the percentage
            defined in self.despikeperc
            """
            LengthToPick = int( float( len( MaxDSortedDInd))* self.despikeperc)
            
            
            """
            If returned LengthToPick is to small take at least the length of SortedDInd
            #### TO BE CHECKED IF THAT IS A GOOD IDEA FOR "good" data
            """
            if( LengthToPick == 0):
                LengthToPick = len( MaxDSortedDInd)
            if( self.debug):
                print( 'len( MaxDSortedDInd): {}'.format( len( MaxDSortedDInd)))
                print( 'LengthToPick: {}'.format( LengthToPick))
            if( self.debug):
                plt.close()
                print( 'MaxInd[ MaxDSortedDInd] is: {}'.format( MaxInd[ MaxDSortedDInd]))
                print( 'MaxInd[ MaxDDSortedDInd] is: {}'.format( MaxInd[ MaxDDSortedDInd]))
                
                
                ###
                # Replacing sorted MaxInd by MaxInd for debugging
                ###
            """
            extend the existing SortedList and FstDiffSortedList with
            SortedDInd and FstSortedDInd respectively in order to return a list
            of lists according to that amount of columns/ lines given
            """
            MaxDSortedList.extend( MaxInd[ MaxDDSortedDInd[:LengthToPick] ])
            ### MaxDSortedList theoretically MaxDSortedList should be filled with
            ### MaxInd[ MaxDSortedDInd[:LengthToPick] ], but since the indices
            ### MaxDSortedDInd are based on self.data and not the DiffData
            ### there would be samples selected which are maxima or minima in
            ### self.data and not DiffData. Since we want to most "off samples"
            ### we need to focus on the DiffData to get those indices which
            ### correspond to them. MaxDSortedDInd on the contrary would
            ### would return those samples with the highest magnitudes of the
            ### original self.data, which is different to those samples, which
            ### are most "off"
            MaxDDSortedList.extend( MaxInd[ MaxDDSortedDInd[:LengthToPick] ])
            if( self.debug):
                #SortedList.extend( MaxInd)
                
                ###
                #print( 'SortedList is: {}'.format( SortedList))
                print( 'd[ np.array( MaxDDSortedList)] is: {}'.format( d[ np.array( MaxDDSortedList)]))
                print( 'od[ np.array( MaxDSortedList)] is: {}'.format( od[ np.array( MaxDSortedList)]))
            """
            convert to arrays  --- maybe remove later, needed for debug eventually
            """
            MaxDSortedList = np.array( MaxDSortedList)
            MaxDDSortedList = np.array( MaxDDSortedList)
            
            
            if( self.debug):
                plt.plot( np.arange( 0, self.len), od, alpha = 0.2, label = 'org')
                plt.plot( np.arange( 0, self.len) + 0.5, d, alpha = 0.2, label = 'diff org  - unbiased')
                #plt.plot( np.arange( 0, self.len), dd, alpha = 0.2, label = 'diffdiff org')
                plt.plot( np.arange( 0, self.len)[MaxDDSortedList] + 0.5, d[MaxDDSortedList], linestyle = ' ', marker = 'x', alpha = 0.7, label = 'diff exceeding - unbiased')
                plt.plot( np.arange( 0, self.len)[MaxDSortedList], od[MaxDSortedList], linestyle = ' ', marker = '+', alpha = 0.7, label = 'org exceeding')
                
                #plt.plot( np.arange( 0, self.len)[MaxDSortedList], dd[MaxDSortedList], linestyle = ' ', marker = '+', alpha = 0.7, label = 'diffdiff exceeding')
                plt.grid( which = 'both')
                plt.legend( loc = 'best')
                plt.show()
                #
                #MaxCond = MaxCond | np.insert( MaxCond, 0, MaxCond[0])[:-1] # shift the true and false by
                # one element to the right and keeo the first element as it is
            self.PercMostOffSamplesList.append( MaxCond)
        
        if( self.axis == 1):
            DiffData, self.data, DiffSignDfData = DiffData.T, self.data.T, DiffSignDfData.T
        #sys.exit()
        
        
        print('\nCalling FindPercMostOffSamples...done')
        return self.PercMostOffSamplesList
    
    
    def DeSpike( self, **kwargs):
        #self.debug = True
        print('\nCalling despiking data...')
        #if( __name__ == "__main__"):
        #self.__init__(**kwargs)
        if( False):
            pass
            if( False):
                try:
                    for item, value in kwargs.items():
                        if( item == 'rplval'):
                            """
                            
                            setting alternative value for replacement type
                            """
                            self.rplval = value
                        if( item == 'lvl'):
                            """
                            
                            setting alternative level for which replacements are beeing
                            applied
                            """
                            self.lvl = value
                        if( item == 'despikeperc'):
                            """
                            
                            setting percentage value of how much datapoints have to valid
                            to apply DeSpike
                            """
                            self.despikeperc = value
                            if( self.despikeperc > 1.0):
                                raise self.MyException( 'Despikeperc has to be <= 1.0...')
                            if( not isinstance( self.despikeperc, float)):
                                raise self.MyException( 'Despikeperc is no float...')
                            else:
                                if( self.despikeperc >= 1.0):
                                    raise self.MyException( 'Despikeperc has to be below 1.0...')
                            if( self.debug):
                                print( 'self.despikeperc in DeSpike routine: {}'.format( self.despikeperc))
                                #sys.exit()
                                
                        if( item == 'data'):
                            """
                            
                            looking for special dataset
                            """
                            self.data = value
                            try:
                                for item, value in kwargs.items():
                                    if( item == 'dt'):
                                        """
                                        
                                        looking for special dataset timestamps
                                        """
                                        self.dt = float( value)
                                        if( self.debug):
                                            print( 'self.dt is: {}'.format( self.dt))
                                        self.len = self.shape[self.axis]
                                        #self.t = np.arange( 0, float( self.len)* self.dt, self.dt)
                                        self.t = np.arange( 0, float( self.len))* self.dt
                                        if( self.debug):
                                            print( 'self.t is: {}'.format( self.t))
                            except:
                                raise self.MyException( 'Got no timestamp for dataset...stopping!')
                        if( item == 'axis'):
                            """
                            
                            looking for special dataset
                            """
                            self.axis = value
                            if( self.debug):
                                print( 'self.axis is: {}'.format( self.axis))
                                #sys.exit()
                except Exception as ex:
                    raise self.MyException( ex)
                try:
                    rplval = self.rplval
                    lvl = self.lvl
                    if( self.debug):
                        print( 'lvl got from self.lvl')
                except Exception as ex:
                    raise self.MyException( ex)
        
        try:
            rplval = self.rplval
            lvl = self.lvl
            if( self.debug):
                print( 'lvl got from self.lvl')
        except Exception as ex:
            raise self.MyException( ex)
        #gc.enable()
        ###########################
        # REPLACE SPIKES WITH DEFINED VALUES FOR VALUES ABOVE lvl
        ###########################
        #print('levels of signal variation which is acceptable:\n\t{}'.format( lvl))
        #chkData = np.gradient( self.data, axis = self.axis)
        self.shape = np.shape( self.data)
        #print( 'self.shape', self.shape)
        self.len = self.shape[self.axis]
        self.t = np.arange( 0, float( self.len))* self.dt
        
        #print( 'self.len', self.len)
        #print( 'np.shape( self.t)', np.shape( self.t))
        #sys.exit()
        N, M = np.shape( self.data)
        if( True):#self.debug):
            print('shape of self.data is: {}x{}'.format( N, M))
            print( 'rplval', rplval)
            print( 'lvl', lvl)
            #sys.exit()
        
        ####
        # USING the second derivative cause this one is nearly trend free and if
        # one artificially adds one sample at the beginning and one at the end
        # of this second derivative series, one gets a series which is equally
        # long as the original series, which simplifies the selection of 
        # sample-spikes
        ####
        Data = np.diff( np.diff( self.data, axis = self.axis), axis = self.axis)
        
        #print( 'np.shape( Data)', np.shape( Data))
        
        
        if( self.axis == 0):
            #Data = self.data.T
            if( self.debug):
                print('shape of Data after np.diff is: {}'.format( np.shape( Data)))
            if( self.debug):
                print('self.axis == 0 shape of Data after np.diff is: {}'.format( np.shape( Data)))
                print( 'shape of Data[-1,:] {}'.format( np.shape( Data[-1,:])))
                print( 'shape of Data {}'.format( np.shape( Data)))
                print( 'np.shape( np.atleast_2d( Data[-1,:])) {}'.format( np.shape( np.atleast_2d( Data[-1,:]))))
                print( 'np.atleast_2d( Data[-1,:]) {}'.format( np.atleast_2d( Data[-1,:])))
                print( 'np.atleast_2d( Data[0,:]) {}'.format( np.atleast_2d( Data[0,:])))
            
            #Data = np.hstack( ( Data.T, Data[-1,:]))
            Data = np.vstack( ( Data, np.atleast_2d( Data[-1,:])))
            if( self.debug):
                print('shape of Data after np.hstack( ( Data, Data[-1,:])) is: {}'.format( np.shape( Data)))
            Data = np.vstack( ( np.atleast_2d( Data[0,:]), Data))
            if( self.debug):
                print('shape of Data after np.vstack( ( np.atleast_2d( Data[0,:]), Data)) is: {}'.format( np.shape( Data)))
                #sys.exit()
            chkavgs = np.nanmedian( Data, axis = self.axis)
            if( self.debug):
                print('chkavgs are: {}'.format( chkavgs))
                #sys.exit()
            #Data = Data.T
            chgData = np.copy( self.data)
            #chkData = chkData.T
        elif( self.axis == 1):
            #Data = self.data
            if( debug):
                print('shape of Data after np.diff is: {}'.format( np.shape( Data)))
            if( debug):
                print('self.axis == 1 shape of Data after np.diff is: {}'.format( np.shape( Data)))
            Data = np.vstack( ( Data.T, Data[:,-1])).T
            Data = np.vstack( ( Data[:,0], Data.T)).T
            #print( 'np.shape( Data)', np.shape( Data))
            
            chkavgs = np.nanmedian( Data, axis = self.axis)
            chgData = np.copy( self.data)
        else:
            raise self.MyException( 'Axis for despiking is chosen to be {} but this axis is not available'.format( self.axis))
        if( self.debug):
            print('shape of Data after stacking is: {}'.format( np.shape( Data)))
            print('shape of ckgavgs after stacking is: {}'.format( np.shape( chkavgs)))
            print('shape of ckgData is: {}'.format( np.shape( chgData)))
        
        
        time = self.t ### need to be defined for later extra-/interpolation
        ####
        # for debugging purposes
        ####
        if( self.debug):
            
            try:
                try:
                    for k, el in enumerate( Data):
                        plt.plot( time, el, alpha = 0.2, label = 'No# {} data'.format( k))
                except:
                    for k, el in enumerate( Data.T):
                        plt.plot( time, el, alpha = 0.2, label = 'No# {} data'.format( k))
                plt.legend( loc = 'best')
                plt.grid( which = 'both')
                plt.show()
            except:
                print( 'shape of time is:\t{}'.format( np.shape( time)))
                print( 'shape of Data is:\t{}'.format( np.shape( Data)))
        if( self.debug):
            print( 'Data is:\t{}\ntime is:\t{}\nrplval is:\t{}\nlvl is:\t{}\n'.format( Data, time, rplval, lvl))
            print( 'shape of Data is:\t{}\nshape of time is:\t{}\n'.format( np.shape( Data), np.shape( time)))
            input()
            #sys.exit()
        
        
        
        ####
        # care for real and imaginary series' too
        ####
        #chkavgs = np.nanmean( chkData, axis = self.axis)
        if( np.isrealobj( self.data)):
            avgs = np.nanmedian( self.data, axis = self.axis)
            navgs = np.zeros( np.shape( avgs))
        elif( np.iscomplexobj( self.data)):
            avgs = np.nanmedian( self.data, axis = self.axis)
            navgs = np.zeros( np.shape( avgs), dtype = complex)
        for k, el in enumerate( avgs):
            if( np.isnan( el) or np.isinf( el)):
                navgs[k] = 0.0
            else:
                navgs[k] = avgs[k]
        avgs = navgs
        del navgs
        #N, M = np.shape( Data)
        
        
        
        #########
        # CONDITIONS BEEING TESTED
        #########
        
        
        changeind = [] # the ones which are NaN, infinite or just spikes in the series'
        mygoodind = [] # the ones which we want to keep as it is
        ####
        # lvl is the manually defined level above which a sample value has to be
        # to be a spike and therefore needs to be removed
        ####
        self.FindPercMostOffSamples()
        
        if( self.axis == 1):
            for d, av, lv, a, od in zip( Data, chkavgs.T, lvl.T, np.array( self.PercMostOffSamplesList), self.data):
                #changeind.append( np.where( np.logical_or( np.logical_or( np.abs( d - av) > lv, np.isnan( d)), np.isinf( d))))
                
                if( self.debug):
                    print( 'd is:\t{}\nav is:\t{}\nlv is:\t{}\n'.format( d, av, lv))
                    print( 'np.shape( d)', np.shape( d))
                """
                derive a mininum and maximum based on the standard deviation of the first difference of self.data
                added and subtracted to self.data but only bad indices where "a" is true and therefore
                bad indices of the first difference
                """
                goodcond = np.invert( a)
                #DevGoodCond = 3.0* np.nanstd( d[goodcond])
                q3, q1 = np.percentile(d[a], self.percvals)
                DevGoodCond = q3 - q1
                """
                deriving a smoothed timeseries from those samples not being a minima or maxima
                """
                IntpSeries = ipol.interp1d( time[ goodcond], od[ goodcond], bounds_error=False, kind=rplval, fill_value = 'extrapolate')(time)
                """
                Deriving a average o 3-element smoothing window of IntpSeries in order to care for
                big spikes in the series' which are not one sample only
                """
                IntpSeries = ( np.insert( IntpSeries, 0, IntpSeries[0])[:-1] + IntpSeries + np.insert( IntpSeries, -1, IntpSeries[0])[1:])/3.0
                if( True):
                    print( 'IntpSeries: {}'.format( IntpSeries))
                    print( 'IntpSeries contains any nan: {}'.format( np.any( np.isnan( IntpSeries))))
                """
                Derive an upper and lower bound for a confidence interval in between which samples are assumed to be trustworthy.
                """
                InptPlus = IntpSeries + DevGoodCond
                InptNega = IntpSeries - DevGoodCond
                
                if( self.debug):
                    print( 'DevGoodCond: {}'.format( DevGoodCond))
                    #plt.plot( time, InptPlus, alpha = 0.2, label = 'No# {} Plus data {} to {} percent'.format( k, self.percvals[0], self.percvals[1]))
                    #plt.plot( time, InptNega, alpha = 0.2, label = 'No# {} Nega data {} to {} percent'.format( k, self.percvals[0], self.percvals[1]))
                    
                    plt.fill_between(time, y1 = InptNega, y2 = InptPlus, alpha = 0.2, label = 'No# {} Nega data {} to {} percent'.format( k, self.percvals[0], self.percvals[1]))
                    plt.plot( time, od, alpha = 0.2, color = 'r', label = 'No# {} original data'.format( k))
                    plt.plot( time, IntpSeries, alpha = 0.3, color = 'g', label = 'No# {} IntpSeries {} to {} percent'.format( k, self.percvals[0], self.percvals[1]))
                    plt.legend( loc = 'best')
                    plt.grid( which = 'both')
                    plt.show()
                f = ( od < InptNega ) | ( od > InptPlus )
                """
                end
                
                
                logical or to improve result of "a" selection
                """
                a = a | f
                b = np.isnan( d) # checking for NaN values
                c = np.isinf( d) # checking for infinite values
                ####
                # NaN and infinite values are beeing replaced by the according
                # average mean values
                ####
                hind = np.argwhere( b | c).flatten()#[0]#.flatten()
                d[hind] = av
                
                ####
                # use the manually defined lvl values to check wether a sample
                # exceeds a given lv or dont. If lv is exceeded the sample will
                # be replaced by self.rplval
                ####
                #a = np.abs( d - av) > lv
                
                #a = ( np.abs( d - av) > lv) & a
                boolnotvec = a | b | c
                ####
                # for those samples where non of conditions b... beeing NaN, 
                # c...beeing a infinite or a ... exceeding a given level lv
                # is fullfilled keep it
                ####
                boolyesvec = np.invert( a | b | c)
                if( self.debug and ( np.any( boolnotvec))):
                    print( 'boolnotvec:\t:{}'.format( boolnotvec))
                    print( 'boolyesvec:\t:{}'.format( boolyesvec))
                    print( 'len of boolnotvec:\t:{}'.format( len( [ f for f in boolnotvec if f == True])))
                    print( 'len of boolyesvec:\t:{}'.format( len( [ f for f in boolyesvec if f == False])))
                    #print( 'Press key to continue...')
                    #sys.stdin.readline()
                    #sys.exit()
                ####
                # extract to samples of the badind
                ####
                e = np.argwhere( boolnotvec).flatten()
                ####
                # extract the samples of the goodind
                ####
                f = np.argwhere( boolyesvec).flatten()
                #print( 'e is:\n\t{}'.format( e))
                if( self.debug and ( np.any( boolnotvec))):
                    print( '\n\n\nf\t=\t{}'.format( f))
                ####
                # check if the amount of samples beeing badind referred to
                # the amount of all samples in self.len is exceeding a given
                # percentage rate self.despikeperc
                ####
                percchk = float( len( e))/float( self.len)
                if( self.debug):
                    print( 'percchk is {}'.format( percchk))
                    
                changeind.append( e)
                mygoodind.append( f)
        if( self.axis == 0):
            for d, av, lv, a, od in zip( Data.T, chkavgs.T, lvl.T, np.array( self.PercMostOffSamplesList), self.data.T):
                #changeind.append( np.where( np.logical_or( np.logical_or( np.abs( d - av) > lv, np.isnan( d)), np.isinf( d))))
                
                if( self.debug):
                    print( 'd is:\t{}\nav is:\t{}\nlv is:\t{}\n'.format( d, av, lv))
                    print( 'np.shape( d)', np.shape( d))
                    print( 'av', av, 'lv', lv)
                    #sys.exit()
                """
                derive a mininum and maximum based on the standard deviation of the first difference of self.data
                added and subtracted to self.data but only bad indices where "a" is true and therefore
                bad indices of the first difference
                """
                goodcond = np.invert( a)
                #DevGoodCond = 3.0* np.nanstd( d[goodcond])
                q3, q1 = np.percentile(d[a], self.percvals)
                DevGoodCond = q3 - q1
                """
                deriving a smoothed timeseries from those samples not being a minima or maxima
                """
                IntpSeries = ipol.interp1d( time[ goodcond], od[ goodcond], bounds_error=False, kind=rplval, fill_value = 'extrapolate')(time)
                """
                Deriving a average o 3-element smoothing window of IntpSeries in order to care for
                big spikes in the series' which are not one sample only
                """
                IntpSeries = ( np.insert( IntpSeries, 0, IntpSeries[0])[:-1] + IntpSeries + np.insert( IntpSeries, -1, IntpSeries[0])[1:])/3.0
                if( True):
                    print( 'IntpSeries: {}'.format( IntpSeries))
                    print( 'IntpSeries contains any nan: {}'.format( np.any( np.isnan( IntpSeries))))
                """
                Derive an upper and lower bound for a confidence interval in between which samples are assumed to be trustworthy.
                """
                InptPlus = IntpSeries + DevGoodCond
                InptNega = IntpSeries - DevGoodCond
                
                if( self.debug):
                    print( 'DevGoodCond: {}'.format( DevGoodCond))
                    #plt.plot( time, InptPlus, alpha = 0.2, label = 'No# {} Plus data {} to {} percent'.format( k, self.percvals[0], self.percvals[1]))
                    #plt.plot( time, InptNega, alpha = 0.2, label = 'No# {} Nega data {} to {} percent'.format( k, self.percvals[0], self.percvals[1]))
                    
                    plt.fill_between(time, y1 = InptNega, y2 = InptPlus, alpha = 0.2, label = 'No# {} Nega data {} to {} percent'.format( k, self.percvals[0], self.percvals[1]))
                    plt.plot( time, od, alpha = 0.2, color = 'r', label = 'No# {} original data'.format( k))
                    plt.plot( time, IntpSeries, alpha = 0.3, color = 'g', label = 'No# {} IntpSeries {} to {} percent'.format( k, self.percvals[0], self.percvals[1]))
                    plt.legend( loc = 'best')
                    plt.grid( which = 'both')
                    plt.show()
                f = ( od < InptNega ) | ( od > InptPlus )
                """
                end
                
                
                logical or to improve result of "a" selection
                """
                a = a | f
                b = np.isnan( d)
                c = np.isinf( d)
                hind = np.argwhere( b | c).flatten()#[0]#.flatten()
                d[hind] = av
                #a = np.abs( d - av) > lv
                
                #a = ( np.abs( d - av) > lv) & a
                boolnotvec = a | b | c
                boolyesvec = np.invert( a | b | c)
                if( self.debug and ( np.any( boolnotvec))):
                    print( 'boolnotvec:\t:{}'.format( boolnotvec))
                    print( 'boolyesvec:\t:{}'.format( boolyesvec))
                    print( 'len of boolnotvec:\t:{}'.format( len( [ f for f in boolnotvec if f == True])))
                    print( 'len of boolyesvec:\t:{}'.format( len( [ f for f in boolyesvec if f == False])))
                    #print( 'Press key to continue...')
                    #sys.stdin.readline()
                    #sys.exit()
                e = np.argwhere( boolnotvec).flatten()
                f = np.argwhere( boolyesvec).flatten()
                #print( 'e is:\n\t{}'.format( e))
                if( self.debug and ( np.any( boolnotvec))):
                    print( '\n\n\nf\t=\t{}'.format( f))
                percchk = float( len( e))/float( self.len)
                if( self.debug):
                    print( 'percchk is {}'.format( percchk))
                    #sys.exit()
                changeind.append( e)
                mygoodind.append( f)
            #print( 'changeind is: {}'.format( changeind[-1]))
        #changeind = np.where( np.abs( Data - np.atleast_2d( avgs).T) > np.atleast_2d( lvl).T)
        #if( self.debug):
            #sys.stdin.readline()
        #if( self.debug):
            #print( 'np.shape( newchgind[k]) is: {}'.format( np.shape( newchgind[k])))
        #    print( 'changeind is: {}'.format( changeind))
        #    print( 'mygoodind is: {}'.format( np.shape( mygoodind)))
        newchgind = np.array( changeind, dtype = 'object')
        mygoodind = np.array( mygoodind, dtype = 'object')
        if( self.debug):
            #print( 'np.shape( newchgind[k]) is: {}'.format( np.shape( newchgind[k])))
            print( 'np.shape( mygoodind) is: {}'.format( np.shape( mygoodind)))
            print( 'np.shape( newchgind) is: {}'.format( np.shape( newchgind)))
            #sys.exit()
        #########
        #newchgind = []
        #for li in changeind:
        #    for el in li:
        #        if( not el in newchgind):
        #            newchgind.append(el[0])
        #newchgind = np.unique( newchgind)
        
        ####
        # for debugging purposes
        ####
        if( self.debug):
            
            try:
                try:
                    for k, ( el, badind, goodind) in enumerate( zip( self.data, newchgind, mygoodind)):
                        IntpSeries = ipol.interp1d( time[ goodind], el[ goodind], bounds_error=False, kind=rplval, fill_value = 'extrapolate')(time)
                        print( 'lvl: {}, badind: {}, badind value: {}'.format( lvl, badind, el[badind]))
                        plt.plot( time, el, alpha = 0.2, label = 'No# {} data'.format( k))
                        plt.plot( time, IntpSeries, alpha = 0.2, label = 'No# {} interpolated data'.format( k))
                        plt.plot( time[badind], el[badind], alpha = 0.7, linestyle = ' ', marker = 'x', markersize = 10, label = 'No# {} data badind'.format( k))
                        plt.legend( loc = 'best')
                        plt.grid( which = 'both')
                        plt.show()
                except:
                    for k, ( el, badind, goodind) in enumerate( zip( self.data.T, newchgind, mygoodind)):
                        IntpSeries = ipol.interp1d( time[ goodind], el[ goodind], bounds_error=False, kind=rplval, fill_value = 'extrapolate')(time)
                        print( 'lvl: {}, badind: {}, badind value: {}'.format( lvl, badind, el[badind]))
                        plt.plot( time, el, alpha = 0.2, label = 'No# {} data'.format( k))
                        plt.plot( time, IntpSeries, alpha = 0.2, label = 'No# {} interpolated data'.format( k))
                        plt.plot( time[badind], el[badind], alpha = 0.7, linestyle = ' ', marker = 'x', markersize = 10, label = 'No# {} data badind'.format( k))
                        plt.legend( loc = 'best')
                        plt.grid( which = 'both')
                        plt.show()

            except:
                print( 'shape of time is:\t{}'.format( np.shape( time)))
                print( 'shape of Data is:\t{}'.format( np.shape( Data)))
            print( 'Data is:\t{}\ntime is:\t{}\nrplval is:\t{}\nlvl is:\t{}\n'.format( Data, time, rplval, lvl))
            print( 'shape of Data is:\t{}\nshape of time is:\t{}\n'.format( np.shape( Data), np.shape( time)))
            #input()
            #sys.exit()
        
        checklenlist = list( chain( *newchgind))
        sumbadind = []
        if( len( checklenlist) >= 1):
            if( self.debug):
                print( 'checklenlist is:\t{}\n'.format( checklenlist))
                print( 'len of checklenlist is:\t{}\n'.format( len( checklenlist)))
                #sys.exit()
                if( self.debug):
                    if( self.axis == 1):
                        for k, el in enumerate( newchgind):
                            print( '\nData[{}]\t=\t{}'.format( k, chgData[k, el.astype(int)]))
                    if( self.axis == 0):
                        for k, el in enumerate( newchgind):
                            print( '\nData[{}]\t=\t{}'.format( k, chgData.T[k, el.astype(int)]))                    
                    #sys.stdin.readline()
            del a, b, c, e, f, boolnotvec, boolyesvec
            if( self.debug):
                print( 'rplval is: {} before'.format( rplval))
                print( 'type of rplval is: {} before'.format( type( rplval)))
            #print( 'shape of newchgind is:\n\t{}'.format( np.shape( newchgind)))
            #print( 'newchgind is:\n\t{}'.format( newchgind))
            if( np.prod( np.shape( newchgind)) > 0):
                if( isinstance( rplval, str)):
                    if( self.debug):
                        print( 'rplval is: {} after'.format( rplval))
                    if( str(rplval).startswith('mean')):
                        for k, ( ind, badind) in enumerate( zip( mygoodind, newchgind)):
                            if( len( ind) > 0):
                                #goodind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in newchgind[k]]
                                #print( 'average of goodind[{}] is:\n\t{}'.format( k, np.mean( d[ goodind])))
                                goodind = np.array( ind, dtype = int)
                            else:
                                goodind = np.arange( 0, [N, M][self.axis])
                            if( self.debug):
                                print( 'shape of goodind: {}'.format( np.shape( goodind)))
                            
                            #badind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in goodind]
                            if( len( badind) > 1):
                                for el in badind:
                                    chkval = el
                                    while(chkval not in goodind and chkval >= 0):
                                        chkval = chkval - 1
                                    lwind = chkval
                                    chkval = el
                                    while(chkval not in goodind and chkval <= [N, M][self.axis]):
                                        chkval = chkval + 1
                                    upind = chkval
                                    if( self.debug):
                                        print( 'lwind: {}'.format( lwind))
                                    #upind = (np.argwhere( el - goodind < 0).flatten())[0]
                                    if( self.debug):
                                        print( 'upind: {}'.format( upind))
                                    wa = np.exp( -np.abs( float( el - lwind)))
                                    we = np.exp( -np.abs( float( el - upind)))
                                    chgData[ k, el] = ( chgData[k, lwind]* wa + chgData[k, upind]* we)/ (wa+we)
                            if( self.debug):
                                print( 'replacing following indexed elements with mean:')
                                print( badind)
                                print( 'shape of chgData before change: {}'.format( np.shape( chgData)))
                            if( self.debug):
                                for n, el in enumerate( chgData[ k, badind]):
                                    if( True):
                                        print( '\nreplaced element[{}] is equal to avgs{}={}'.format(n, k, el))
                            sumbadind.append( badind)
                    #elif( rplval.startswith('int')): GOOD TO HAVE BUT NOT IMPORTANT AT THE MOMENT
                    #    
                    elif( str(rplval).startswith('na') or rplval.startswith('Na') or rplval.startswith('NA') or rplval.startswith('nA')):
                        for k, (ind, badind) in enumerate( zip( mygoodind, newchgind)):
                            if( len( ind) > 0):
                                #goodind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in newchgind[k]]
                                #print( 'average of goodind[{}] is:\n\t{}'.format( k, np.mean( d[ goodind])))
                                goodind = ind
                            else:
                                goodind = np.arange( 0, [N, M][self.axis])
                            #badind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in goodind]
                            #print tplind[0]
                            #print tplind[1]
                            #Data[ k, badind] = np.nan
                            chgData[ k, badind] = np.nan
                            sumbadind.append( badind)
                    
                    
                    
                    elif( str( rplval).startswith( 'cubic') or str( rplval).startswith( 'linear')):
                        if( np.isrealobj(chgData)):
                            temp = np.zeros( np.shape( chgData))
                        elif( np.iscomplexobj(chgData)):
                            temp = np.zeros( np.shape( chgData), dtype = complex)
                        if( self.axis == 1):
                            if( self.debug):
                                print( 'self.axis', self.axis)
                            for k, (d, ind, badind) in enumerate( zip( chgData, mygoodind, newchgind)):
                                ind = np.array( ind, dtype = int)
                                #print( 'k is: {}'.format( k))
                                #print( 'shape of d in Data is: {}'.format( np.shape( d)))
                                if( self.debug):
                                    #print( 'np.shape( newchgind[k]) is: {}'.format( np.shape( newchgind[k])))
                                    print( 'np.shape( mygoodind[{}]) is: {}'.format( k, np.shape( ind)))
                                #if( len( newchgind[k]) > 0):
                                if( np.nanmax( np.shape( ind)) > 0):
                                    #goodind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in newchgind[k]]
                                    #print( 'average of goodind[{}] is:\n\t{}'.format( k, np.mean( d[ goodind])))
                                    goodind = ind
                                    print( 'for column {} there are {} good indices'.format( k, np.nanmax( np.shape( ind))))
                                else:
                                    goodind = np.arange( 0, [N, M][self.axis])
                                #badind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in goodind]
                                sumbadind.append( badind)
                                if( self.debug):
                                    print( 'shape of goodind is: {}'.format( np.shape( goodind)))
                                    print( 'shape of d is: {}'.format( np.shape( d)))
                                    print( 'shape of time is: {}'.format( np.shape( time)))
                                    print( 'len( goodind) != [N, M][self.axis]', len( goodind) != [N, M][self.axis])
                                if( len( goodind) != [N, M][self.axis]):
                                    #if( str( rplval).startswith( 'linear')):
                                    #    pl = np.polyfit( time[ goodind], d[ goodind], 1)
                                    #elif( str( rplval).startswith( 'cubic')):
                                    #    pl = np.polyfit( time[ goodind], d[ goodind], 3)
                                    if( self.debug):
                                        print( 'shape of time:\t{}'.format( np.shape( time)))
                                        print( 'shape of goodind:\t{}'.format( np.shape( goodind)))
                                        print( 'shape of d:\t{}'.format( np.shape( d)))
                                        print( 'goodind:\t{}'.format( goodind))
                                    temp[k,:] = ipol.interp1d( time[ goodind], d[ goodind], bounds_error=False, kind=rplval, fill_value = 'extrapolate')(time)
                                    #dummy = np.polyval( pl, time[badind])
                                    #sortorder = np.argsort( np.hstack( ( time[goodind], time[badind])))
                                    #print( 'sortorder is: {}'.format( sortorder))
                                    #if( True):
                                    #    plt.plot( sortorder)
                                    #    plt.show()
                                    #dummy = np.hstack( ( d[goodind], dummy))[sortorder]
                                    #temp[k, :] = dummy
                                else:
                                    temp[k, :] = d
                                    #pass
                                if( np.any( np.isnan( temp[k,:])) and self.debug):
                                    #print( "[{}]-th column has some nan's".format( k, d))
                                    print( 'Colums with nans:\n\n\n')
                                    for n, el in enumerate( temp[k,:]):
                                        if( np.isnan(el) or np.isinf( el)):
                                            print( '\n\tel[{}]\t=\t{}'.format( n, el))
                                            print( '\n\td[{}]\t=\t{}'.format( n, d[n]))
                                    #sys.stdin.readline()
                                    #plt.plot( time, temp[k, :])
                                    #plt.show()
                                    #sys.exit()
                                if( np.any( np.isnan( temp[k,:]))):
                                    #print( "[{}]-th column has some nan's".format( k, d))
                                    print( 'Colums with nans...\n\n\n')
                                    raise self.MyException( "[{}]-th column has some nan's for temp[k,:]: {}".format( k, d))
                                #input("Press Enter to continue...")
                            chgData = temp
                        if( self.axis == 0):
                            if( self.debug):
                                print( 'self.axis', self.axis)
                            for k, (d, ind, badind) in enumerate( zip( chgData.T, mygoodind, newchgind)):
                                ind = np.array( ind, dtype = int)
                                #print( 'k is: {}'.format( k))
                                #print( 'shape of d in Data is: {}'.format( np.shape( d)))
                                if( self.debug):
                                    #print( 'np.shape( newchgind[k]) is: {}'.format( np.shape( newchgind[k])))
                                    print( 'shape of d in chgData is: {}'.format( np.shape( d)))
                                    print( 'np.shape( mygoodind[{}]) is: {}'.format( k, np.shape( ind)))
                                #if( len( newchgind[k]) > 0):
                                if( np.nanmax( np.shape( ind)) > 0):
                                    #goodind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in newchgind[k]]
                                    #print( 'average of goodind[{}] is:\n\t{}'.format( k, np.mean( d[ goodind])))
                                    goodind = ind
                                    print( 'for column {} there are {} good indices'.format( k, np.nanmax( np.shape( ind))))
                                else:
                                    goodind = np.arange( 0, [N, M][self.axis])
                                #badind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in goodind]
                                sumbadind.append( badind)
                                if( self.debug):
                                    print( 'shape of goodind is: {}'.format( np.shape( goodind)))
                                    print( 'shape of d is: {}'.format( np.shape( d)))
                                    print( 'shape of time is: {}'.format( np.shape( time)))
                                    print( 'len( goodind) != [N, M][self.axis]', len( goodind) != [N, M][self.axis])
                                if( len( goodind) != [N, M][self.axis]):
                                    #if( str( rplval).startswith( 'linear')):
                                    #    pl = np.polyfit( time[ goodind], d[ goodind], 1)
                                    #elif( str( rplval).startswith( 'cubic')):
                                    #    pl = np.polyfit( time[ goodind], d[ goodind], 3)
                                    if( self.debug):
                                        print( 'shape of time:\t{}'.format( np.shape( time)))
                                        print( 'shape of goodind:\t{}'.format( np.shape( goodind)))
                                        print( 'shape of d:\t{}'.format( np.shape( d)))
                                    #temp[k,:] = ipol.interp1d( time[ goodind], d[ goodind], bounds_error=False, kind=rplval, fill_value = 'extrapolate')(time)
                                    temp[:, k] = ipol.interp1d( time[ goodind], d[ goodind], bounds_error=False, kind=rplval, fill_value = 'extrapolate')(time)
                                    #dummy = np.polyval( pl, time[badind])
                                    #sortorder = np.argsort( np.hstack( ( time[goodind], time[badind])))
                                    #print( 'sortorder is: {}'.format( sortorder))
                                    #if( True):
                                    #    plt.plot( sortorder)
                                    #    plt.show()
                                    #dummy = np.hstack( ( d[goodind], dummy))[sortorder]
                                    #temp[k, :] = dummy
                                else:
                                    temp[:, k] = d
                                    #pass
                                if( np.any( np.isnan( temp[:, k])) and self.debug):
                                    #print( "[{}]-th column has some nan's".format( k, d))
                                    print( 'Colums with nans:\n\n\n')
                                    for n, el in enumerate( temp[:, k]):
                                        if( np.isnan(el) or np.isinf( el)):
                                            print( '\n\tel[{}]\t=\t{}'.format( n, el))
                                            print( '\n\td[{}]\t=\t{}'.format( n, d[n]))
                                    #sys.stdin.readline()
                                    #plt.plot( time, temp[k, :])
                                    #plt.show()
                                    #sys.exit()
                                if( np.any( np.isnan( temp[:, k]))):
                                    #print( "[{}]-th column has some nan's".format( k, d))
                                    print( 'Colums with nans...\n\n\n')
                                    raise self.MyException( "[{}]-th column has some nan's for temp[k,:]: {}".format( k, d))
                                #input("Press Enter to continue...")
                            chgData = temp
                        
                        
                        
                        del temp
                elif( isinstance( rplval, float)):
                    for k, ( ind, badind) in enumerate( zip( mygoodind, newchgind)):
                        if( len( ind) > 0):
                            goodind = ind
                        else:
                            goodind = np.arange( 0, [N, M][self.axis])
                        #badind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in goodind]
                        chgData[ k, badind] = rplval
                        for el in badind:
                            sumbadind.append( el)
                else:
                    #print( 'wrong replacement value...stopping!')
                    #print( "[{}]-th column has some nan's".format( k, d))
                    raise self.MyException( 'wrong replacement value:\t{}...stopping!'.format( rplval))
        else:
            print('\nNo values to change...continuing\n')
            #pass
        if( self.debug):
            if( self.axis == 1):
                for el, bl in zip( chgData, self.data):
                    plt.plot( self.t, el, label = 'despiked data')
                    plt.plot( self.t, bl, label = 'original data')
                    plt.legend( loc ='best')
                    plt.grid( which = 'both')
                    plt.show()
            elif( self.axis == 0):
                for el, bl in zip( chgData.T, self.data.T):
                    plt.plot( self.t, el, label = 'despiked data')
                    plt.plot( self.t, bl, label = 'original data')
                    plt.legend( loc ='best')
                    plt.grid( which = 'both')
                    plt.show()
                    
        #self.data = Data
        if( self.axis == 0):
            self.despdata = chgData
            #chkData = chkData.T
        elif( self.axis == 1):
            self.despdata = chgData
        else:
            raise self.MyException( 'Error during transposition to original dimensions with axis {}'.format( self.axis))
        if( self.debug):
            print( 'shape of despdata is now {}'.format( np.shape( self.despdata)))
        allchgindlen = len( list( chain(*sumbadind)))
        print( '\n\n\n{} datasamples replaced by {} replacement value'.format( allchgindlen, rplval))
        #inloopvarlist = [f[0] for f in list( locals().iteritems())]
        #selectlist = ['DeSpikedData', 'changeind']
        #dellist = [f for f in inloopvarlist if f not in selectlist]
        #sys.exit()
        #for f in dellist:
        #    del f
        #gc.collect(2)
        #del gc.garbage[:]
        if( self.debug):
            print( 'displaying self.despdata...')
            print( 'self.despdata', self.despdata)
            plt.plot( self.despdata.flatten(), label = 'despiked data')
            plt.grid( which = 'both')
            plt.legend( loc = 'best')
            plt.show()
            print( 'displaying self.despdata...done')
        self.newchgind = newchgind
        return self.despdata, self.newchgind
    
    
    
    
    #def FIELDAUGMENT( tseries, aseries):
    def FIELDAUGMENT( self, *args):
        """
        @author: Niko Kompein, Idea by Ramon Egli
        
        FUNCTION FOR ARTIFICIAL AUGMENTATION OF TIMESERIES' TO BE PERIODIC
        tseries ... timeseries array
        tseries ... amplitude-series array
        
        both in numpy format
        """
        from numpy import shape, argmin, argmax, hstack, log2, ceil, floor, power, atleast_2d
        try: ### oldschool python
            from numpy import float, int, arange, max, nanmean, vstack, mod#, polyfit, polyval
        except: ### python3 3.12+ python
            from numpy import arange, max, nanmean, vstack, mod#, polyfit, polyval
            from numpy import float64 as float
            from numpy import int64 as int
        from numpy import zeros
        try: ### oldschool python
            from scipy.signal import parzen#, exponential
            from scipy import polyfit, polyval
        except: ### python3 3.12+ python
            from scipy.signal.windows import parzen#, exponential
            from numpy import polyfit, polyval
        
        
        
        if( self.debug):
            print( 'len( args)', len( args))
            print( 'args', args)
        if( len( args) == 2):
            
            aseries = args[1]
            tseries = args[0]
            if( self.debug):
                print( 'shape of aseries is:\t{}'.format( shape( aseries)))
                print( 'shape of tseries is:\t{}'.format( shape( tseries)))
            if( aseries.ndim == 1):
                aseries = atleast_2d( aseries)
            M, N = shape(aseries)
            minDim, maxDim = [ argmin( [M, N]), argmax( [M, N])]
            #length = [ M, N][maxDim]
            length = [ M, N][self.axis]
            columns = [ M, N][not self.axis]
            if( self.debug):
                print( 'Dimensions of aseries: \n M, N:{},{}'.format( M, N ))
                print( 'Dimensions of aseries: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
                print( 'shape of aseries is:\t{}'.format( shape( aseries)))
                print( 'shape of tseries is:\t{}'.format( shape( tseries)))
                print( 'length of aseries is:\t{}'.format( length))
                #sys.exit()
            
            #if( M > N):
            #    aseries = aseries.T # ROTATE IF aseries has more rows than columns
            #####
            # RECHECK DIMENSIONS
            #####
            M, N = shape(aseries)
            minDim, maxDim = [ argmin( [M, N]), argmax( [M, N])]
            #length = [ M, N][maxDim]
            length = [ M, N][self.axis]
            columns = [ M, N][not self.axis]
            #print( 'Dimensions of aseries after transposition: \n M, N:{},{}'.format( M, N ))
            #print( 'Dimensions of aseries after transposition: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
            
            
            ########
            # Remove mean
            ########
            if( False):
                aseries = aseries - nanmean( aseries, axis = 0).T
            
            
            
            
            dt = ( tseries[-1] - tseries[0])/( float( length) - 1.0)
            if( self.debug):
                print( 'sampling interval:{}'.format( dt))
                print( 'length of tseries:{}'.format( max( shape( tseries))))
                print( 'length of aseries is:\t{}'.format( length))
            #print( 'length of aseries:{}'.format( max( shape( aseries))))
            
            if( self.axis == 1):
                arev = aseries[:, ::-1] # reversed aseries for use in three_aseries
                #print( 'shape of arev', np.shape( arev))
                #print( 'shape of aseries', np.shape( aseries))
                three_aseries = hstack(( arev, aseries[:, 1::], arev[:, 1::])) # three time long amplitude-series for further processing
            elif( self.axis == 0):
                arev = aseries[::-1, :] # reversed aseries for use in three_aseries
                three_aseries = vstack(( arev, aseries[1::, :], arev[1::, :])) # three time long amplitude-series for further processing
            #print( 'shape of three_aseries', np.shape( three_aseries))
            del arev
            if( self.debug):
                print( 'sampling interval:{}'.format( dt))
                print( 'length of three_aseries:{}'.format( max( shape( three_aseries))))
                print( 'shape of three_aseries is:\t{}'.format( np.shape( three_aseries)))
                print( 'shape of tseries is:\t{}'.format( np.shape( tseries)))
                #sys.exit()
            #print( 'length of three_aseries:{}'.format( max( shape( three_aseries))))
            
            U, V = shape(three_aseries) # dimensions of three_aseries
            minDim, maxDim = [ argmin( [U, V]), argmax( [U, V])]
            three_length = [ U, V][self.axis]
            three_columns = [ U, V][not self.axis]
            
            atseries = tseries[0] + arange( tseries[0] - tseries[-1] , 2.0*( tseries[-1] - tseries[0]), dt)
            #atseries = tseries[0] + arange( tseries[0] - tseries[-1] , 2.0*( tseries[-1] - tseries[0]) + dt, dt)
            #print( 'shape of atseries is:\t{}'.format( np.shape( atseries)))
            #print( 'Dimensions of three_aseries: \n U, V:{},{}'.format( U, V ))
            #print( 'Dimensions of three_aseries: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
            #print( 'atseries is: {}'.format( atseries))
            #print( 'tseries[-1] - tseries[0] is: {}'.format( tseries[-1] - tseries[0]))
            #print( '2 * (tseries[-1] - tseries[0]) is: {}'.format( 2.0*( tseries[-1] - tseries[0])))
            #print( 'length of atseries:{}'.format( max( shape( atseries))))
            if( self.debug):
                print( 'three_length is: {}'.format( three_length))
                print( 'three_columns is: {}'.format( three_columns))
            if( self.debug):
                # only for debugging
                import matplotlib.pyplot as plt 
                import sys
                k = 0
                if( self.axis == 1):
                    tas = (three_aseries - np.atleast_2d( nanmean( three_aseries, axis = self.axis)).T)
                    aas = (aseries - np.atleast_2d( nanmean( aseries, axis = self.axis)).T)
                elif( self.axis == 0):
                    tas = (three_aseries - np.atleast_2d( nanmean( three_aseries, axis = self.axis)))
                    aas = (aseries - np.atleast_2d( nanmean( aseries, axis = self.axis)))
                print( 'np.shape( tas)', np.shape( tas))
                print( 'np.shape( atseries)', np.shape( atseries))
                print( 'np.shape( tseries)', np.shape( tseries))
                print( 'np.shape( aseries)', np.shape( aseries))
                if( self.axis == 0):
                    plt.plot( atseries, tas[1:,k], 'r')
                    plt.plot( tseries, aas[:,k], 'g')
                elif( self.axis == 1):
                    plt.plot( atseries, tas[k,1:], 'r')
                    plt.plot( tseries, aas[k,:], 'g')
                plt.show()
                #sys.exit()
            
            
            if( self.debug):
                print( 'three_length is: {}'.format( three_length))
                print( 'length is: {}'.format( length))
                print( '1.5*length is: {}'.format( 1.5*length))
            
            cutlen = int( 1.5*length)
            pow2len = int( power( 2.0, float( ceil( log2( float( cutlen)))))) # new length multiple of power 2
            #halfpow2len = int( float( pow2len)/2.0)
            if( self.debug):
                print( 'pow2len is: {}'.format( pow2len))
            if( mod( cutlen, 2) != 0):
                remlen = three_length - pow2len + 1 # REMAIN LENGTH OF TIMESERIES WHICH WILL NOT BE RETURNED
            else:
                remlen = three_length - pow2len # REMAIN LENGTH OF TIMESERIES WHICH WILL NOT BE RETURNED
            
            firsthalfremlen = int( floor( float( remlen)/2.0))
            secondhalfremlen = int( ceil( float( remlen)/2.0))
            if( self.debug):
                print( 'cutlen is: {}'.format( cutlen))
                print( 'pow2len is: {}'.format( pow2len))
                print( 'remlen is: {}'.format( remlen))
                print( 'firsthalfremlen is: {}'.format( firsthalfremlen))
                print( 'secondhalfremlen is: {}'.format( secondhalfremlen))

            ######
            # CHECK IF BOTH LENGTHS firsthalfremlen and secondhalfremlen TOGETHER ARE remlen long and correct if neccesary
            ######
            if( remlen != firsthalfremlen + secondhalfremlen):
                secondhalfremlen = secondhalfremlen - 1
                if( remlen != firsthalfremlen + secondhalfremlen):
                    secondhalfremlen = secondhalfremlen + 2
            if( self.debug):
                print( 'firsthalfremlen is: {}'.format( firsthalfremlen))
                print( 'secondhalfremlen is: {}'.format( secondhalfremlen))
                print( 'remlen is: {}'.format( remlen))
                print( 'three_length - secondhalfremlen is: {}'.format( three_length - secondhalfremlen))
            #aaseries = three_aseries[:, firsthalfremlen + 1:firsthalfremlen + 1 + pow2len:]#  CUT augmented timeseries so that it is as long as aaseries in the end
            if( self.axis == 1):
                aaseries = three_aseries[:, firsthalfremlen + 1:firsthalfremlen + 1 + pow2len:]#  CUT augmented timeseries so that it is as long as aaseries in the end
            elif( self.axis == 0):
                aaseries = three_aseries[firsthalfremlen + 1:firsthalfremlen + 1 + pow2len:, :]#  CUT augmented timeseries so that it is as long as aaseries in the end
            orgStartInd = int( length) - ( firsthalfremlen + 2)
            #orgStartInd, orgEndInd = int( length) - ( firsthalfremlen + 2), int( cutlen - 1) - ( firsthalfremlen) - 1 # REMEMBER indices of original timestamps tseries[0] and tseries[-1] in atseries 
            orgEndInd = orgStartInd + length # REMEMBER indices of original timestamps tseries[0] and tseries[-1] in atseries 
            #orgStartInd, orgEndInd = int( length) - ( firsthalfremlen + 1), int( 2* length -1) - ( firsthalfremlen) # REMEMBER indices of original timestamps tseries[0] and tseries[-1] in atseries 
            if( self.debug):
                print( 'orgStartind is:\n\t{}\norgEndInd is:\n\t{}'.format( orgStartInd, orgEndInd))
            if( self.axis == 1):
                if( max( shape( aaseries)) != pow2len): # only if i'm completedly confused
                    aaseries = three_aseries[:, firsthalfremlen + 1:firsthalfremlen + 2 + pow2len:]
            elif( self.axis == 0):
                if( max( shape( aaseries)) != pow2len): # only if i'm completedly confused
                    aaseries = three_aseries[firsthalfremlen + 1:firsthalfremlen + 2 + pow2len:, :]                
            atseries = atseries[firsthalfremlen + 1:firsthalfremlen + 1 + pow2len:] # CUT augmented timeseries so that it is as long as aaseries in the end
            #print( 'atseries is: {}'.format( atseries))
            #print( 'shape of atseries is: {}'.format( shape( atseries)))
            #print( 'aaseries is: {}'.format( aaseries))
            #print( 'shape of aaseries is: {}'.format( shape( aaseries)))
            ####
            # detrending boundary timeranges
            ####
            if( self.axis == 1):
                temp = vstack( ( aaseries[:,0], aaseries[:,-1])).T
                #print( 'shape of temp is: \n:{}'.format( shape( temp) ))
                avg_int_lvls = nanmean( temp, axis = self.axis) 
            elif( self.axis == 0):
                temp = vstack( ( aaseries[0,:].T, aaseries[-1,:].T))
                #print( 'shape of temp is: \n:{}'.format( shape( temp) ))
                avg_int_lvls = nanmean( temp, axis = self.axis)                 
            if( self.debug):
                print( 'temp', temp)
                print( 'avg_int_lvls', avg_int_lvls)
                print( 'shape( avg_int_lvls)', shape( avg_int_lvls))
                print( 'shape( aaseries)', shape( aaseries))
                print( 'shape( temp)', shape( temp))
                #sys.exit()
            
            del temp
            

            #aaseries = (aaseries.T - avg_int_lvls).T
            
            if( False):
                # only for debugging
                import matplotlib.pyplot as plt 
                import sys
                k = 2
                plt.plot( atseries, (aaseries.T - nanmean( aaseries[:, orgStartInd: orgEndInd], axis = 1))[:,k], 'r')
                plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                plt.show()
                sys.exit()
            
            
            D = 40.0 # dezibel decay over half of window length
            #taplen =  orgStartInd + ( max( shape( aaseries)) - orgEndInd + 1) + 2
            taplen = 2* orgStartInd + 1
            if( self.debug):
                print( 'taplen is: {}'.format( taplen))
            tauval = float( taplen)/2.0 * 8.69/ D # https://en.wikipedia.org/wiki/Window_function from Exponential or Poisson window
            #taper = exponential( taplen, tau = tauval)
            #if( np.isrealobj(avg_int_lvls)):
            taper = parzen( taplen)
            #elif( np.iscomplexobj(avg_int_lvls)):
            #    taper = parzen( taplen, dtype = complex)
            maxind = argmax( taper)
            halftplen = int( floor( max( shape( taper))/2.0))
            
            #######
            # REMOVE TREND AT BEGINING
            #######
            trendind = [0, orgStartInd]
            if( self.debug):
                print( 'trendind is:\t{}'.format( trendind))
                print( 'shape of aaseries is:\t{}'.format( np.shape( aaseries)))
                print( 'M is:\t{}'.format( M))
                print( '[M,N][not self.axis] is:\t{}'.format( [M,N][not self.axis]))
            if( self.axis == 1):
                if( np.isrealobj(avg_int_lvls)):
                    #temp = np.zeros( np.shape( chgData))
                    trendfct = zeros( ( columns, 1 + trendind[-1] - trendind[0]))
                elif( np.iscomplexobj(avg_int_lvls)):
                    #temp = np.zeros( np.shape( chgData))
                    trendfct = zeros( ( columns, 1 + trendind[-1] - trendind[0]), dtype = complex)
            elif( self.axis == 0):
                if( np.isrealobj(avg_int_lvls)):
                    #temp = np.zeros( np.shape( chgData))
                    trendfct = zeros( ( 1 + trendind[-1] - trendind[0], columns))
                elif( np.iscomplexobj(avg_int_lvls)):
                    #temp = np.zeros( np.shape( chgData))
                    trendfct = zeros( ( 1 + trendind[-1] - trendind[0], columns), dtype = complex)
            #trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
            if( self.debug):
                print( 'trendfct is:\t{}'.format( trendfct))
                print( 'shape of trendfct is:\t{}'.format( np.shape( trendfct)))
                #print( 'aaseries[ :, trendind]', aaseries[ :, trendind])
                #print( 'np.shape( aaseries[ :, trendind])', np.shape( aaseries[ :, trendind]))
            if( self.axis == 1):
                for k, a in enumerate( (aaseries[ :, trendind].T ).T): #  - aaseries[:,trendind[-1]]
                    if( debug):
                        print( 'atseries[trendind]', atseries[trendind])
                        print( '[avg_int_lvls[k], 0.0]', [avg_int_lvls[k], 0.0])
                        #print( 'len( atseries)', len( atseries))
                        #print( 'len( aaseries)', len( aaseries))
                        import matplotlib.pyplot as plt
                        plt.plot( atseries)
                        plt.grid( which = 'both')
                        plt.show()
                    polynom = polyfit( atseries[trendind], [avg_int_lvls[k], 0.0], 1, rcond = power( 10.0, -40.0))
                    trendfct[k, :] = polyval( polynom, atseries[trendind[0]:trendind[-1] + 1])# + avg_int_lvls[k]
            elif( self.axis == 0):
                for k, a in enumerate( (aaseries[ trendind, :] )): #  - aaseries[:,trendind[-1]]
                    #print( 'avg_int_lvls[k]', avg_int_lvls[k])
                    #print( 'atseries[trendind[0]:trendind[-1] + 1]', atseries[trendind[0]:trendind[-1] + 1])
                    #print( 'np.shape( atseries[trendind[0]:trendind[-1] + 1])', np.shape( atseries[trendind[0]:trendind[-1] + 1]))
                    polynom = polyfit( atseries[trendind], [avg_int_lvls[k], 0.0], 1, rcond = power( 10.0, -40.0))
                    trendfct[:, k] = polyval( polynom, atseries[trendind[0]:trendind[-1] + 1])# + avg_int_lvls[k]
            if( self.debug):
                print( 'shape of trendfct is: {}'.format( shape( trendfct)))
                print( 'np.shape( aaseries[trendind[0]:trendind[-1] + 1, :])', np.shape( aaseries[trendind[0]:trendind[-1] + 1, :]))
            #sys.exit()
            #for f,g in zip( trendind, trendfct):
            #    print( 'trendfct[{}] is: {}'.format( f, g[f - 1]))
            if( self.axis == 1):
                aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] - trendfct
                #print( 'aaseries after removal of beginings trend: {}'.format( aaseries[:,0:orgStartInd]))
                #aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[:halftplen + 1] # TAPERING
                try:
                    aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[:maxind] # TAPERING
                except:
                    aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[:maxind + 1] # TAPERING
            elif( self.axis == 0):
                aaseries[trendind[0]:trendind[-1] + 1, :] = aaseries[trendind[0]:trendind[-1] + 1, :] - trendfct
                #print( 'aaseries after removal of beginings trend: {}'.format( aaseries[:,0:orgStartInd]))
                #aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[:halftplen + 1] # TAPERING
                try:
                    aaseries[trendind[0]:trendind[-1] + 1, :] = aaseries[trendind[0]:trendind[-1] + 1, :] * taper[:maxind] # TAPERING
                except:
                    aaseries[trendind[0]:trendind[-1] + 1, :] = aaseries[trendind[0]:trendind[-1] + 1, :] * np.atleast_2d( taper[:maxind + 1]).T # TAPERING
            
            G, H = shape(aaseries) # dimensions of three_aseries
            minDim, maxDim = [ argmin( [G, H]), argmax( [G, H])]
            pow2_length = [ G, H][self.axis]
            pow2_columns = [ G, H][not self.axis]
            
            if( False):
                # only for debugging
                import matplotlib
                matplotlib.use('TkAgg')
                import matplotlib.pyplot as plt 
                import sys
                k = 2
                #plt.plot( atseries, (aaseries.T - nanmean( aaseries[:, orgStartInd: orgEndInd], axis = 1))[:,k], 'r')
                plt.plot( atseries, aaseries.T[:,k], 'r')
                #plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                plt.plot( atseries[trendind[0]:trendind[-1] + 1], trendfct[k,:], 'b')
                plt.show()
                sys.exit()
            
            #######
            # REMOVE TREND AT ENDING
            #######
            #trendind = [ orgEndInd - 2, pow2_length - 1]
            trendind = [ orgEndInd, pow2_length - 1]
            if( self.debug):
                print( 'orgEndInd:{}\npow2_length:{}\nM:{}'.format( orgEndInd, pow2_length, M))
            #trendind = [ orgEndInd -3, pow2_length - 1]
            if( self.axis == 1):
                if( np.isrealobj(avg_int_lvls)):
                    #temp = np.zeros( np.shape( chgData))
                    trendfct = zeros( ( pow2_columns, 1 + trendind[-1] - trendind[0]))
                elif( np.iscomplexobj(avg_int_lvls)):
                    #temp = np.zeros( np.shape( chgData))
                    trendfct = zeros( ( pow2_columns, 1 + trendind[-1] - trendind[0]), dtype = complex)
                #trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
                for k, a in enumerate( (aaseries[:, trendind].T ).T):
                    polynom = polyfit( atseries[trendind], [0.0, avg_int_lvls[k]], 1, rcond = power( 10.0, -40.0))
                    trendfct[k, :] = polyval( polynom, atseries[trendind[0]:trendind[-1] + 1])
                #print( 'shape of trendfct is: {}'.format( shape( trendfct)))
                #for a, f,g in zip( aaseries, trendind, trendfct):
                #    print( 'aaseries[{}] trendfct[{}] is: {}'.format( a[f], f, g[f - 1 - orgEndInd]))
                aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] - trendfct
            elif( self.axis == 0):
                if( np.isrealobj(avg_int_lvls)):
                    #temp = np.zeros( np.shape( chgData))
                    trendfct = zeros( ( 1 + trendind[-1] - trendind[0], pow2_columns))
                elif( np.iscomplexobj(avg_int_lvls)):
                    #temp = np.zeros( np.shape( chgData))
                    trendfct = zeros( ( 1 + trendind[-1] - trendind[0], pow2_columns), dtype = complex)
                #trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
                for k, a in enumerate( (aaseries[trendind, ]).T):
                    polynom = polyfit( atseries[trendind], [0.0, avg_int_lvls[k]], 1, rcond = power( 10.0, -40.0))
                    trendfct[:, k] = polyval( polynom, atseries[trendind[0]:trendind[-1] + 1])
                #print( 'shape of trendfct is: {}'.format( shape( trendfct)))
                #for a, f,g in zip( aaseries, trendind, trendfct):
                #    print( 'aaseries[{}] trendfct[{}] is: {}'.format( a[f], f, g[f - 1 - orgEndInd]))
                aaseries[trendind[0]:trendind[-1] + 1, :] = aaseries[trendind[0]:trendind[-1] + 1, :] - trendfct
            #print( 'aaseries after removal of endings trend: {}'.format( aaseries[:,trendind[0]:trendind[-1]]))
            #aaseries[:,trendind[0]:trendind[-1]] = aaseries[:,trendind[0]:trendind[-1]] * taper[halftplen + 1::] # TAPERING
            #print( 'trendind[0]:trendind[-1] is: {} : {}\n trendind[-1] - trendind[0] is: {}'.format( trendind[0], trendind[-1], trendind[-1] - trendind[0]))
            #print( 'shape of taper[maxind::] is: {}'.format( shape( taper[maxind::])))
            #try:
            #    aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[maxind + 1::] # TAPERING
            #except:
            #    aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[maxind + 2::] # TAPERING
            #try:
            if( self.debug):
                print( 'aaseries[:,trendind[0]:trendind[-1] + 1]', aaseries[:,trendind[0]:trendind[-1] + 1])
                print( 'aaseries[trendind[0]:trendind[-1] + 1, :]', aaseries[trendind[0]:trendind[-1] + 1, :])
                print( 'taper[ -(trendind[-1] - trendind[0]) - 1::]', taper[ -(trendind[-1] - trendind[0]) - 1::])
                print( 'np.shape( aaseries[:,trendind[0]:trendind[-1] + 1])', np.shape( aaseries[:, trendind[0]:trendind[-1] + 1]))
                print( 'np.shape( aaseries[trendind[0]:trendind[-1] + 1, :])', np.shape( aaseries[trendind[0]:trendind[-1] + 1, :]))
                print( 'np.shape( taper[ -(trendind[-1] - trendind[0]) - 1::])', np.shape( taper[ -(trendind[-1] - trendind[0]) - 1::]))
            if( self.axis == 1):
                aaseries[ :, trendind[0]:trendind[-1] + 1] = aaseries[ :, trendind[0]:trendind[-1] + 1] * taper[ -( 1 + trendind[-1] - trendind[0])::] # TAPERING
            elif( self.axis == 0):
                aaseries[trendind[0]:trendind[-1] + 1, :] = aaseries[trendind[0]:trendind[-1] + 1, :] * np.atleast_2d( taper[ -( 1 + trendind[-1] - trendind[0])::]).T # TAPERING
                #aaseries[trendind[0]:trendind[-1] + 1, :] = aaseries[trendind[0]:trendind[-1] + 1, :] * np.atleast_2d( taper[maxind::]).T # TAPERING
                #taper[:maxind + 1]
            #except:
            #    aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[maxind + 2::] # TAPERING
    
            
            #aaseries = (aaseries.T + avg_int_lvls).T
            
            
            if( self.debug):
                # only for debugging
                import matplotlib
                matplotlib.use('TkAgg')
                import matplotlib.pyplot as plt 
                import sys
                #k = 2
                k = 0
                #plt.plot( atseries, (aaseries.T - nanmean( aaseries[:, orgStartInd: orgEndInd], axis = 1))[:,k], 'r')
                if( self.axis == 1):
                    plt.plot( atseries, aaseries.T[:,k], 'r')
                    #plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                    plt.plot( atseries[trendind[0]:trendind[-1] + 1], trendfct[k,:], 'b')
                    plt.plot( atseries[trendind[0]+1:trendind[-1]], taper[maxind::]* np.max( trendfct[k,:]), 'g')
                elif( self.axis == 0):
                    plt.plot( atseries, aaseries.T[k,:], 'r')
                    #plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                    plt.plot( atseries[trendind[0]:trendind[-1] + 1], trendfct[:,k], 'b')
                    #plt.plot( atseries[trendind[0]:trendind[-1] + 1], taper[maxind + 1::]*max( trendfct[:,k]), 'g')                    
                plt.show()
                #sys.exit()
            
            
            #print( 'Dimensions of augmented timeseries: \n G, H:{},{}'.format( G, H ))
            #print( 'Dimensions of augmented timeseries: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
            #print( 'Dimensions of augmented timeseries: \n pow2_length, pow2_columns:{},{}'.format( pow2_length, pow2_columns ))
            #print( 'sampling interval:{}'.format( dt))
            if( self.debug):
                print( 'length of augmented timeseries (atseries):{}'.format( max( shape( atseries))))
            self.augt = atseries
            if( np.argmax( np.shape( aaseries)) != self.axis):
                self.augdata = aaseries.T
            else:
                self.augdata = aaseries
            self.orgStartInd = orgStartInd
            self.orgEndInd = orgEndInd
            self.len = len( self.augt)
            self.fmin = 1.0/ float( self.len)/ self.dt
            self.AmplitudeScale = np.sqrt( self.fmin* self.dt**2.0)
            if( self.debug):
                print( 'shape of self.augt is:\t{}'.format( shape( self.augt)))
                print( 'shape of self.augdata is:\t{}'.format( shape( self.augdata)))
                print( 'self.orgStartInd is:\t{}'.format( self.orgStartInd))
                print( 'self.orgEndInd is:\t{}'.format( self.orgEndInd))
            return atseries, aaseries, orgStartInd, orgEndInd
        
        """
        If the FIELDAUGMENTATION is called as an argument of another function an artificial timeseries with sampling interval of
        1.0 second will be used
        """
        
        if( len( args) == 1):
            
            
            aseries = args[0]
            
            M, N = shape(aseries)
            minDim, maxDim = [ argmin( [M, N]), argmax( [M, N])]
            #length = [ M, N][maxDim]
            length = [M, N][maxDim]
            columns = [ M, N][minDim]
            print( 'Dimensions of aseries: \n M, N:{},{}'.format( M, N ))
            print( 'Dimensions of aseries: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
            print( 'length of aseries: \n length:{}'.format( length))
            if( self.debug):
                print( 'shape of aseries is:\t{}'.format( shape( aseries)))
                print( 'shape of tseries is:\t{}'.format( shape( tseries)))
            if( M > N):
                aseries = aseries.T # ROTATE IF aseries has more rows than columns
            #####
            # RECHECK DIMENSIONS
            #####
            M, N = shape(aseries)
            minDim, maxDim = [ argmin( [M, N]), argmax( [M, N])]
            length = [ M, N][maxDim]
            columns = [ M, N][minDim]
            #print( 'Dimensions of aseries after transposition: \n M, N:{},{}'.format( M, N ))
            #print( 'Dimensions of aseries after transposition: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
            
            
            ########
            # Remove mean
            ########
            if( False):
                aseries = aseries - nanmean( aseries, axis = 0).T
            
            
            tseries = arange(0.0, float( N) + 1.0, 1.0)
            
            dt = ( tseries[-1] - tseries[0])/( float( length) - 1.0)
            if( self.debug):
                print( 'sampling interval:{}'.format( dt))
                print( 'length of tseries:{}'.format( max( shape( tseries))))
            #print( 'length of aseries:{}'.format( max( shape( aseries))))
            
            arev = aseries[:, ::-1] # reversed aseries for use in three_aseries
            three_aseries = hstack(( arev, aseries[:, 1::], arev[:,1::])) # three time long amplitude-series for further processing
            del arev
            
            #print( 'length of three_aseries:{}'.format( max( shape( three_aseries))))
            
            U, V = shape(three_aseries) # dimensions of three_aseries
            minDim, maxDim = [ argmin( [U, V]), argmax( [U, V])]
            three_length = [ U, V][maxDim]
            three_columns = [ U, V][minDim]
            atseries = tseries[0] + arange( tseries[0] - tseries[-1] , 2.0*( tseries[-1] - tseries[0]) + dt, dt)
            #print( 'Dimensions of three_aseries: \n U, V:{},{}'.format( U, V ))
            #print( 'Dimensions of three_aseries: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
            #print( 'atseries is: {}'.format( atseries))
            #print( 'tseries[-1] - tseries[0] is: {}'.format( tseries[-1] - tseries[0]))
            #print( '2 * (tseries[-1] - tseries[0]) is: {}'.format( 2.0*( tseries[-1] - tseries[0])))
            #print( 'length of atseries:{}'.format( max( shape( atseries))))
            
            if( self.debug):
                # only for debugging
                import matplotlib.pyplot as plt 
                import sys
                k = 2
                plt.plot( atseries, (three_aseries.T - nanmean( three_aseries, axis = 1))[:,k], 'r')
                plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                plt.show()
                sys.exit()
            
            pow2len = int( power( 2.0, float( ceil( log2( float( length)))))) # new length multiple of power 2
            #halfpow2len = int( float( pow2len)/2.0)
            
            remlen = three_length - pow2len # REMAIN LENGTH OF TIMESERIES WHICH WILL NOT BE RETURNED
            
            
            firsthalfremlen = int( floor( float( remlen)/2.0))
            secondhalfremlen = int( ceil( float( remlen)/2.0))
            
            
            ######
            # CHECK IF BOTH LENGTHS firsthalfremlen and secondhalfremlen TOGETHER ARE remlen long and correct if neccesary
            ######
            if( remlen != firsthalfremlen + secondhalfremlen):
                secondhalfremlen = secondhalfremlen - 1
                if( remlen != firsthalfremlen + secondhalfremlen):
                    secondhalfremlen = secondhalfremlen + 2
            if( self.debug):
                print( 'firsthalfremlen is: {}'.format( firsthalfremlen))
                print( 'secondhalfremlen is: {}'.format( secondhalfremlen))
                print( 'remlen is: {}'.format( remlen))
                print( 'three_length - secondhalfremlen is: {}'.format( three_length - secondhalfremlen))
            #aaseries = three_aseries[:, firsthalfremlen + 1:firsthalfremlen + 1 + pow2len:]#  CUT augmented timeseries so that it is as long as aaseries in the end
            aaseries = three_aseries[:, firsthalfremlen + 1:firsthalfremlen + 1 + pow2len:]#  CUT augmented timeseries so that it is as long as aaseries in the end
            orgStartInd, orgEndInd = int( length) - ( firsthalfremlen + 1), int( 2*length - 1) - ( firsthalfremlen) # REMEMBER indices of original timestamps tseries[0] and tseries[-1] in atseries 
            #orgStartInd, orgEndInd = int( length) - ( firsthalfremlen + 1), int( 2* length -1) - ( firsthalfremlen) # REMEMBER indices of original timestamps tseries[0] and tseries[-1] in atseries 
            if( max( shape( aaseries)) != pow2len): # only if i'm completedly confused
                aaseries = three_aseries[:, firsthalfremlen + 1:firsthalfremlen + 2 + pow2len:]
            atseries = atseries[firsthalfremlen + 1:firsthalfremlen + 1 + pow2len:] # CUT augmented timeseries so that it is as long as aaseries in the end
            #print( 'atseries is: {}'.format( atseries))
            #print( 'shape of atseries is: {}'.format( shape( atseries)))
            #print( 'aaseries is: {}'.format( aaseries))
            #print( 'shape of aaseries is: {}'.format( shape( aaseries)))
            ####
            # detrending boundary timeranges
            ####
            temp = vstack( ( aaseries[:,0], aaseries[:,-1])).T
            #print( 'shape of temp is: \n:{}'.format( shape( temp) ))
            avg_int_lvls = nanmean( temp, axis = 1) 
            del temp
            
            #aaseries = (aaseries.T - avg_int_lvls).T
            
            if( self.debug):
                # only for debugging
                import matplotlib.pyplot as plt 
                import sys
                k = 2
                plt.plot( atseries, (aaseries.T - nanmean( aaseries[:, orgStartInd: orgEndInd], axis = 1))[:,k], 'r')
                plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                plt.show()
                sys.exit()
            
            
            D = 40.0 # dezibel decay over half of window length
            taplen =  orgStartInd + ( max( shape( aaseries)) - orgEndInd + 1) + 2
            if( self.debug):
                print( 'taplen is: {}'.format( taplen))
            tauval = float( taplen)/2.0 * 8.69/ D # https://en.wikipedia.org/wiki/Window_function from Exponential or Poisson window
            #taper = exponential( taplen, tau = tauval)
            #if( np.isrealobj(avg_int_lvls)):
            taper = parzen( taplen)
            #elif( np.iscomplexobj(avg_int_lvls)):
            #taper = parzen( taplen, dtype = complex)
            #taper = parzen( taplen)
            maxind = argmax( taper)
            halftplen = int( floor( max( shape( taper))/2.0))
            
            #######
            # REMOVE TREND AT BEGINING
            #######
            trendind = [0, orgStartInd]
            if( np.isrealobj(avg_int_lvls)):
                #temp = np.zeros( np.shape( chgData))
                trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
            elif( np.iscomplexobj(avg_int_lvls)):
                #temp = np.zeros( np.shape( chgData))
                trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]), dtype = complex)
            #trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
            for k, a in enumerate( (aaseries[:, trendind].T ).T): #  - aaseries[:,trendind[-1]]
                polynom = polyfit( atseries[trendind], [avg_int_lvls[k], 0.0], 1, rcond = power( 10.0, -40.0))
                trendfct[k, :] = polyval( polynom, atseries[trendind[0]:trendind[-1] + 1])# + avg_int_lvls[k]
            #print( 'shape of trendfct is: {}'.format( shape( trendfct)))
            #for f,g in zip( trendind, trendfct):
            #    print( 'trendfct[{}] is: {}'.format( f, g[f - 1]))
            aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] - trendfct
            #print( 'aaseries after removal of beginings trend: {}'.format( aaseries[:,0:orgStartInd]))
            #aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[:halftplen + 1] # TAPERING
            try:
                aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[:maxind] # TAPERING
            except:
                aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[:maxind + 1] # TAPERING
            
            G, H = shape(aaseries) # dimensions of three_aseries
            minDim, maxDim = [ argmin( [G, H]), argmax( [G, H])]
            pow2_length = [ G, H][maxDim]
            pow2_columns = [ G, H][minDim]
            
            if( self.debug):
                # only for debugging
                import matplotlib
                matplotlib.use('TkAgg')
                import matplotlib.pyplot as plt 
                import sys
                k = 2
                #plt.plot( atseries, (aaseries.T - nanmean( aaseries[:, orgStartInd: orgEndInd], axis = 1))[:,k], 'r')
                plt.plot( atseries, aaseries.T[:,k], 'r')
                #plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                plt.plot( atseries[trendind[0]:trendind[-1] + 1], trendfct[k,:], 'b')
                plt.show()
                sys.exit()
            
            #######
            # REMOVE TREND AT ENDING
            #######
            trendind = [ orgEndInd - 2, pow2_length - 1]
            #trendind = [ orgEndInd -3, pow2_length - 1]
            if( np.isrealobj(avg_int_lvls)):
                #temp = np.zeros( np.shape( chgData))
                trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
            elif( np.iscomplexobj(avg_int_lvls)):
                #temp = np.zeros( np.shape( chgData))
                trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]), dtype = complex)
            #trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
            for k, a in enumerate( (aaseries[:, trendind].T ).T):
                polynom = polyfit( atseries[trendind], [0.0, avg_int_lvls[k]], 1, rcond = power( 10.0, -40.0))
                trendfct[k, :] = polyval( polynom, atseries[trendind[0]:trendind[-1] + 1])
            #print( 'shape of trendfct is: {}'.format( shape( trendfct)))
            #for f,g in zip( trendind, trendfct):
            #    print( 'trendfct[{}] is: {}'.format( f, g[f - 1 - orgEndInd]))
            aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] - trendfct
            #print( 'aaseries after removal of endings trend: {}'.format( aaseries[:,trendind[0]:trendind[-1]]))
            #aaseries[:,trendind[0]:trendind[-1]] = aaseries[:,trendind[0]:trendind[-1]] * taper[halftplen + 1::] # TAPERING
            #print( 'trendind[0]:trendind[-1] is: {} : {}\n trendind[-1] - trendind[0] is: {}'.format( trendind[0], trendind[-1], trendind[-1] - trendind[0]))
            #print( 'shape of taper[maxind::] is: {}'.format( shape( taper[maxind::])))
            try:
                aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[maxind + 1::] # TAPERING
            except:
                aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[maxind + 2::] # TAPERING
    
            
            #aaseries = (aaseries.T + avg_int_lvls).T
            
            
            if( self.debug):
                # only for debugging
                import matplotlib
                matplotlib.use('TkAgg')
                import matplotlib.pyplot as plt 
                import sys
                k = 2
                k = 2
                #plt.plot( atseries, (aaseries.T - nanmean( aaseries[:, orgStartInd: orgEndInd], axis = 1))[:,k], 'r')
                plt.plot( atseries, aaseries.T[:,k], 'r')
                #plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                plt.plot( atseries[trendind[0]:trendind[-1] + 1], trendfct[k,:], 'b')
                plt.plot( atseries[trendind[0]:trendind[-1] + 1], taper[maxind + 1::]*max( trendfct[k,:]), 'g')
                plt.show()
                sys.exit()
            
            
            #print( 'Dimensions of augmented timeseries: \n G, H:{},{}'.format( G, H ))
            #print( 'Dimensions of augmented timeseries: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
            #print( 'Dimensions of augmented timeseries: \n pow2_length, pow2_columns:{},{}'.format( pow2_length, pow2_columns ))
            #print( 'sampling interval:{}'.format( dt))
            if( self.debug):
                print( 'length of augmented timeseries (atseries):{}'.format( max( shape( atseries))))
            self.augt = atseries
            self.augdata = aaseries.T
            self.orgStartInd = orgStartInd
            self.orgEndInd = orgEndInd
            self.len = len( self.augt)
            self.fmin = 1.0/ float( self.len)/ self.dt
            self.AmplitudeScale = np.sqrt( self.fmin* self.dt**2.0)
            if( self.debug):
                print( 'shape of self.augt is:\t{}'.format( shape( self.augt)))
                print( 'shape of self.augdata is:\t{}'.format( shape( self.augdata)))
                print( 'self.orgStartInd is:\t{}'.format( self.orgStartInd))
                print( 'self.orgEndInd is:\t{}'.format( self.orgEndInd))
            return atseries, aaseries.T, orgStartInd, orgEndInd
    
    
    
    
    
    def __myFFTproc__( self):
        if( self.despike):
            print( 'Despiking data...')
            self.DeSpike()
            self.data, changeind = self.despdata, self.newchgind
            if( self.debug):
                plt.close()
                plt.plot( self.t, self.data.T, alpha = 0.2, label = 'self.data after DeSpike routine')
                plt.show()
                #sys.exit()
        if( self.aug):
            """ if there is some field augmentation applied"""
            #self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.FIELDAUGMENT( self.t, self.data)
            self.avg = np.nanmean( self.data, axis = self.axis)
            if( self.axis == 0):
                self.avg = np.atleast_2d( self.avg)
                self.FIELDAUGMENT( self.t, self.data - self.avg)
            if( self.axis == 1):
                #self.FIELDAUGMENT( self.t, self.data.T - self.avg)
                self.avg = np.atleast_2d( self.avg).T
                self.FIELDAUGMENT( self.t, self.data - self.avg)
                #self.augdata = self.augdata.T
            print( '\n\nField augmentation successfully applied')
        else:
            """ if there is no field augmentation applied"""
            self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.t, self.data, 0, self.shape[self.axis] - 1
        if( self.debug):
            print( 'shape of self.t is:\t{}'.format( np.shape( self.t)))
            print( 'shape of self.augt is:\t{}'.format( np.shape( self.augt)))
            print( 'shape of self.augdata is:\t{}'.format( np.shape( self.augdata)))
            print( 'self.orgStartInd is:\t{}'.format( self.orgStartInd))
            print( 'self.orgEndInd is:\t{}'.format( self.orgEndInd))
            #sys.exit()
        """
        
        derive frequency axis cause field-augmentation has to be concerned
        
        """
        
        self.len = np.shape( self.augt)[0]
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.fmin = 1.0/ float( self.len)/ self.dt
        
        if( self.debug):
            print( 'self.fmin is: {}'.format( self.fmin))
        self.__faxis__()
        self.fmin = np.min( self.faxis[self.posind])
        if( self.debug):
            print( 'self.fmin is: {}'.format( self.fmin))
            #sys.exit()
        if( False):
            if( self.PeriodAxis):
                #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
                self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
                self.posind = np.argwhere( self.faxis >= 1.0/ self.fnyq).flatten()
                self.negind = np.argwhere( self.faxis <= -1.0/ self.fnyq).flatten()
            else:
                #self.faxis = np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
                self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
                if( self.debug):
                    print( 'self.faxis is: {}'.format( self.faxis))
                    print( 'shape of self.faxis is: {}'.format( np.shape( self.faxis)))
                a = self.faxis >= self.fmin
                b = self.faxis <= self.fnyq
                c = self.faxis <= -self.fmin
                d = self.faxis >= -self.fnyq
                if( self.debug):
                    print( '\na\t=\t{}\nb\t=\t{}\nc\t=\t{}\nd\t=\t{}'.format( a, b, c, d))
                    print( '\nself.faxis[2050]\t=\t{}'.format( self.faxis[2050]))
                if( self.debug):
                    print( 'shape of original timeseries posind: {}'.format( np.shape( self.posind)))
                self.posind = np.argwhere( a & b).flatten()#[0]
                self.negind = np.argwhere( c & d).flatten()#[0]
                if( self.debug):
                    print( 'shape of aufgmented posind: {}'.format( np.shape( self.posind)))
                    #sys.exit()
            if( self.debug):
                print( 'self.faxis, self.posind, self.negind are:\n{},\n{},\n{}'.format( self.faxis, self.posind, self.negind))
        
        #N = np.shape( self.augt)[0]
        if( self.aug):
            self.taper = np.ones( ( self.len,))
        else:
            try:
                tempwin = sg.windows.hanning( self.len)
            except:
                tempwin = sg.windows.hann( self.len)
            self.taper = tempwin
        
        if( self.data.ndim == 2):
            if( self.axis != np.max( np.shape( self.taper))):
                self.taper = np.atleast_2d( self.taper).T
        if( self.debug):
            print( 'shape of self.taper is:\t{}'.format( np.shape( self.taper)))
        if( self.axis == 0):
            dummy = ( self.augdata * self.taper) # tapering
        elif( self.axis == 1):
            dummy = ( self.augdata.T * self.taper).T # tapering
        #dummy = self.augdata
        if( self.debug):
            print( 'shape of product of self.augdata and self.taper is:\t{}'.format( np.shape( dummy)))
            print( 'shape of self.data:\t{}'.format( np.shape( self.data)))
        self.fdata = fft.fft( dummy, axis = self.axis)* self.AmplitudeScale # * np.sqrt( self.fmin* self.dt**2.0) # Scaling with np.sqrt( self.fmin* self.dt**2.0) to ensure unit of A/sqrt( Hz)
        #self.fdata = fft.fft( dummy, axis = self.axis) * np.sqrt( self.fmin* self.dt)
        self.fdata = fft.fftshift( self.fdata, axes = self.axis)
        if( np.argmax( np.shape( self.fdata)) != np.argmax( np.shape( self.data))):
            print( 'np.shape( self.fdata)', np.shape( self.fdata))
            print( 'np.shape( self.data)', np.shape( self.data))
            #sys.exit()
            self.fdata = self.fdata.T
            print( 'np.shape( self.fdata) transposed: ', np.shape( self.fdata))
            #sys.exit()
        if( self.debug):
            print( 'shape of self.fdata is:\t{}'.format( np.shape( self.fdata)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            if( self.axis == 0):
                plt.loglog( self.faxis[self.posind], np.abs( self.fdata[self.posind, :]))
            elif( self.axis == 1):
                plt.loglog( self.faxis[self.posind], np.abs( self.fdata[:, self.posind]).T)
            plt.grid(True)
            plt.show()
        #sys.exit()
        return self.faxis, self.fdata
    
    
    
    def ArtExtendSpec( self, FrqSeries):
        OrgFrqSeries = np.copy( FrqSeries)
        actlen = len( OrgFrqSeries)
        bakactlen = np.copy( actlen)
        
        #ArtFaxis = fft.fftshift(fft.fftfreq(len( FrqSeries), d = self.dt))
        ArtFaxis = fft.fftfreq( len( FrqSeries)* 2 - 1, d = self.dt).astype(np.float64) # only positive frequencies
        ArtFaxis = ArtFaxis[ np.argwhere( ArtFaxis >= 0.0).flatten()]
        if( self.debug):
            print( 'ArtFaxis: {}'.format( ArtFaxis))
            print( 'len of ArtFaxis: {}'.format( len( ArtFaxis)))
            print( 'ArtFaxis.dtype: {}'.format( ArtFaxis.dtype))            
            #sys.exit()
        """
        artifically extend the frequency-series, so that one can derive
        a NxN-matrix of it ... extension will be removed at the end
        
        Zitat: Google gemini: 
            https://share.google/aimode/Q1AZ0bTVj4VEFfX6z
        Während die PCA im Zeitbereich empfindlich auf 
        Phasenverschiebungen reagiert (ein verschobenes Signal wirkt 
        wie ein anderes Muster), ist die FFT (bzw. das 
        Amplitudenspektrum) phaseninvariant. Die PCA auf der FFT 
        findet daher: Wiederkehrende Akkorde oder Klangfarben: Wenn 
        Ihr Signal ein System ist, das in verschiedenen Fenstern immer 
        wieder dieselbe Kombination von Frequenzen erzeugt (z.B. ein 
        Grundton mit bestimmten Oberschwingungen), erkennt die PCA 
        diese Kombination als einen einzigen Eigenvektor. Spektrale 
        Texturen: Sie finden Muster, die über das gesamte Frequenzband 
        verteilt sind, aber immer gemeinsam auftreten (z.B. das 
        spezifische Frequenzprofil eines Maschinenschadens).
        
        """
        n = 2
        #print( 'np.sqrt( float( actlen)): {}'.format( np.sqrt( float( actlen))))
        while( np.mod( float( actlen), np.sqrt( actlen)) != 0):
            if( self.debug):
                print( 'shape of FrqSeries: {}'.format( np.shape( FrqSeries)))
            FrqSeries = np.hstack( (FrqSeries, FrqSeries[-n:-n+1]))
            #FrqSeries = np.hstack( ( FrqSeries[n-1:n], FrqSeries))
            n = n + 1
            actlen = len( FrqSeries)
        if( self.debug):
            print( 'shape of FrqSeries after artificial extension: {}'.format( np.shape( FrqSeries)))
        
        SqrtLen = int( np.floor( np.sqrt( float( actlen))))
        
        
        
        """
        Reshape to square array but care for multiples of fundamental mode
        frequency
        """
        #NSer = []
        #Nfaxis = []
        #o = 0
        
        #nonzerocond = ArtFaxis# != 0.0
        #nonzeroind = np.argwhere( nonzerocond).flatten()
        myAx = ArtFaxis#[nonzeroind]
        #print( 'myAx min: {}, myAx max: {}'.format( np.min( myAx), np.max( myAx)))
        #input()
        #difflen = actlen - len( myAx) # length difference between artificially
        # extended frq-series and myAx length for later usage in windowing
        
        reshapedFrqSeries = FrqSeries.reshape( SqrtLen, SqrtLen)
        #sys.exit()
        ApproxSerLen = int( SqrtLen** 2.0)
        if( self.debug):
            Nfaxis = myAx[0:bakactlen]
            
            NSer = reshapedFrqSeries.reshape( ApproxSerLen)[0:bakactlen]
            print( 'NSer: {}'.format( NSer))
            #Nfaxis, UniInd = np.unique( Nfaxis, return_index = True)
            #NSer = np.array( NSer, dtype = complex).flatten()[UniInd]
            print( 'shape of Nfaxis: {}'.format( np.shape( Nfaxis)))
            print( 'shape of NSer: {}'.format( np.shape( NSer)))
            #for k, ( f, el) in enumerate( zip( Nfaxis, NSer)):
            #    print( 'freq: {}, sample: {}'.format( f, el))
            #    if( np.mod(k, 363) == 0):
            #        input()
            #cond, ind = np.unique( ArtFaxis, return_index = True)
            #print( 'every ArtFaxis is unique: {}'.format( np.all( cond)))
            plt.loglog( Nfaxis, np.abs( NSer), alpha = 0.3, label = 'NSer')
            plt.loglog( ArtFaxis, np.abs( OrgFrqSeries), alpha = 0.3, label = 'OrgFrqSeries')
            #plt.plot( ArtFaxis[1:] - ArtFaxis[:-1], label = 'ArtFaxis[1:] - ArtFaxis[:-1]')
            plt.grid( which = 'both')
            plt.legend( loc = 'best')
            plt.show()
            sys.exit()
        
        
        """
        remove average mean before deriving SVD... assuming column vectors
        """
        if( True):
            AvgTrend = np.nanmean( reshapedFrqSeries, axis = 0)
        else:
            AvgTrend = []
            lgax = ArtFaxis
            #plt.close()
            #print( 'reshapedFrqSeries[:,0]', reshapedFrqSeries[:,0])
            for k, row in enumerate( reshapedFrqSeries.T):
                IndToTake = np.arange( k, k+SqrtLen)
                cond = ~np.isnan( lgax[IndToTake]) & np.isfinite( lgax[IndToTake]) & ~np.isnan( row) & np.isfinite( row)
                
                #print( 'lgax[IndToTake]', lgax[IndToTake])
                #print( 'row', row)
                #input()
                IpRow = np.polyval( np.polyfit( lgax[IndToTake][cond], row[cond], deg = 1), lgax[IndToTake])
                AvgTrend.append( IpRow)
                #plt.plot( lgax[IndToTake], row, label = 'row', alpha = 0.3)
                #plt.plot( lgax[IndToTake], IpRow, label = 'IpRow', alpha = 0.3)
                #plt.legend( loc = 'best')
                #plt.show()
            AvgTrend = np.array( AvgTrend)
        
        
        
        
        FrqSeries = reshapedFrqSeries - AvgTrend
        del reshapedFrqSeries
        
        
        
        
        ###
        # Use Singular value decomposition to get the eigenvectors and values
        ###
        print( 'performing np.linalg.svd...')
        vecs, vals, Vt = np.linalg.svd( FrqSeries, full_matrices=False)
        print( 'performing np.linalg.svd...done')
        FrqSeries = FrqSeries + AvgTrend
        # 4. Reconstruct (even with 100% components, this preserves phase-magnitude alignment)
        #X_reconstructed = U @ np.diag(S) @ Vt
        
        
        ###
        # take those eigenvalues/ vectors which above a -3dB bound ref.
        # to the maximum value of eigenvalues
        ###
        eigvalMax = np.max( np.abs( vals))
        #eigvalMax = np.min( np.abs( vals))
        
        normedVals = np.log10( vals/ eigvalMax)
        
        cond = normedVals >= -4.0
        #cond = np.isfinite( normedVals)# >= -4.0
        
        ind = np.argwhere( cond).flatten()
        ####
        vals = vals[ind]
        norm = np.linalg.norm( vecs)
        vecs = vecs#/ norm
        vecs = vecs[:, ind]
        Vt = Vt[ind, :]
        ###
        # done
        ###
        
        if( self.debug):
            print( 'vecs: {}\tVt: {}\nvals: {}\nnormedvals: {}'.format( vecs, Vt, vals, normedVals))
            #sys.exit()
        
        return ind, vecs, vals, Vt, FrqSeries, ApproxSerLen
    
    
    
    def PCARotate( self, FrqSeries):
        
        """
        trying to derive the most prominent Eigenvalues of a series
        based on a approx. squared matrix of FrqSeries and rotate perform
        a PCA rotation
        """
        BakFrqSeries = np.copy( FrqSeries)
        bakShape = np.shape( BakFrqSeries)
        Maxind = [-99, -99]
        self.debug = False
        AllPCAFrqSeries = []
        if( BakFrqSeries.ndim == 1):
            BakFrqSeries = np.atleast_2d( BakFrqSeries)
        
        Maxvecs = [[0],[1]]
        Maxvals = [[0],[1]]
        MaxVt = [[0],[1]]
        Artfaxis = fft.rfftfreq( len( FrqSeries), d = self.dt) # only positive frequencies
        if( self.debug):
            print( 'Maxvecs[0]: {}, Maxvecs[1]: {}'.format( Maxvecs[0], Maxvecs[1]))
            print( 'Maxvals[0]: {}, Maxvals[1]: {}'.format( Maxvals[0], Maxvals[1]))
            print( 'MaxVt[0]: {}, MaxVt[1]: {}'.format( MaxVt[0], MaxVt[1]))
            
            
            Maxvecs[0] = np.eye( int( np.floor( np.sqrt( len( FrqSeries)))))
            Maxvecs[1] = -np.eye( int( np.floor( np.sqrt( len( FrqSeries)))))*5
            
            Maxvals[0] = np.ones( ( int( np.floor( np.sqrt( len( FrqSeries)))), ), dtype = complex)
            Maxvals[1] = -np.ones( ( int( np.floor( np.sqrt( len( FrqSeries)))), ), dtype = complex)*3.0
            
            MaxVt[0] = np.eye( int( np.floor( np.sqrt( len( FrqSeries)))), dtype = complex)
            MaxVt[1] = -np.eye( int( np.floor( np.sqrt( len( FrqSeries)))), dtype = complex)*6.0

            print( 'changed for testing...')
            print( 'Maxvecs[0]: {}, Maxvecs[1]: {}'.format( Maxvecs[0], Maxvecs[1]))
            print( 'Maxvals[0]: {}, Maxvals[1]: {}'.format( Maxvals[0], Maxvals[1]))
            print( 'MaxVt[0]: {}, MaxVt[1]: {}'.format( MaxVt[0], MaxVt[1]))
            sys.exit()
        
        
        if( self.axis == 1):
            for FrqSeries in BakFrqSeries:
                
                OrgFrqSeries = np.copy( np.abs( FrqSeries))
                
                actlen = len( OrgFrqSeries)
                bakactlen = np.copy( actlen)
                """
                artifically extend the frequency-series, so that one can derive
                a NxN-matrix of it ... extension will be removed at the end
                
                Zitat: Google gemini: 
                    https://share.google/aimode/Q1AZ0bTVj4VEFfX6z
                Während die PCA im Zeitbereich empfindlich auf 
                Phasenverschiebungen reagiert (ein verschobenes Signal wirkt 
                wie ein anderes Muster), ist die FFT (bzw. das 
                Amplitudenspektrum) phaseninvariant. Die PCA auf der FFT 
                findet daher: Wiederkehrende Akkorde oder Klangfarben: Wenn 
                Ihr Signal ein System ist, das in verschiedenen Fenstern immer 
                wieder dieselbe Kombination von Frequenzen erzeugt (z.B. ein 
                Grundton mit bestimmten Oberschwingungen), erkennt die PCA 
                diese Kombination als einen einzigen Eigenvektor. Spektrale 
                Texturen: Sie finden Muster, die über das gesamte Frequenzband 
                verteilt sind, aber immer gemeinsam auftreten (z.B. das 
                spezifische Frequenzprofil eines Maschinenschadens).
                
                """
                
                
                """
                positive frequencies
                """
                #print( 'self.posind: {}'.format( self.posind))
                indPos, vecsPos, valsPos, VtPos, PCAPrep, ApproxSerLen = self.ArtExtendSpec( FrqSeries[self.posind])
                
                if( self.debug):
                    #print( 'covmat is: {}'.format( covmat))
                    #print( 'shape of covmat is: {}'.format( np.shape( covmat)))
                    #print( 'shape of vals is: {}'.format( np.shape( vals)))
                    print( 'shape of vecsPos is: {}'.format( np.shape( vecsPos)))
                
                
                
                
            
                """
                eigenvalues(…, M) array ... vals
        
                The eigenvalues, each repeated according to its multiplicity. 
                The eigenvalues are ordered due to SVD usage. The resulting 
                array will be of complex type. When a is real the resulting 
                eigenvalues will be real (0 imaginary part) or occur in 
                conjugate pairs.
        
                eigenvectors(…, M, M) array ... Vt
        
                The normalized (unit “length”) eigenvectors, such that the 
                column eigenvectors[i, :] is the eigenvector corresponding to 
                the eigenvalue eigenvalues[i].
        
                
                rotate data with found eigenvectors
                Use Vt.conj().T @ Vt to get a square form of the eigenvectors
                in order to get the original matrix form
                """
                if( self.debug):
                    print( 'shape of FrqSeries: {}'.format( np.shape( FrqSeries)))
                    print( 'shape of VtPos: {}'.format( np.shape( VtPos)))
                Inner = PCAPrep@ VtPos.conj().T
                if( self.debug):
                    print( 'shape of Inner: {}'.format( np.shape( Inner)))
                
                
                
                
                #print( 'SqrtLen: {}'.format( SqrtLen))
                #print( 'SqrtLen**2.0: {}'.format( SqrtLen**2.0))
                #input()
                
                #!!! this needs to be done in reverse after applying the PCA rotation
                """
                reshape the PCA rotated matrix to original ndim = 1 array form
                """
                
                PCAFrqSeries = ( (Inner)@ VtPos).reshape( 1, ApproxSerLen)#
                
                
                
                """
                reshape the PCA rotated matrix to original ndim = 1 array form done
                """
                """
                negative frequencies
                """
                #print( 'self.faxis[ self.negind]: {}'.format( self.faxis[ self.negind]))
                indNeg, vecsNeg, valsNeg, VtNeg, PCAPrep, ApproxSerLen = self.ArtExtendSpec( FrqSeries[self.negind])
                
                if( self.debug):
                    #print( 'covmat is: {}'.format( covmat))
                    #print( 'shape of covmat is: {}'.format( np.shape( covmat)))
                    #print( 'shape of vals is: {}'.format( np.shape( vals)))
                    print( 'shape of vecsNeg is: {}'.format( np.shape( vecsNeg)))
                
                
                
                
            
                """
                eigenvalues(…, M) array ... vals
        
                The eigenvalues, each repeated according to its multiplicity. 
                The eigenvalues are ordered due to SVD usage. The resulting 
                array will be of complex type. When a is real the resulting 
                eigenvalues will be real (0 imaginary part) or occur in 
                conjugate pairs.
        
                eigenvectors(…, M, M) array ... Vt
        
                The normalized (unit “length”) eigenvectors, such that the 
                column eigenvectors[i, :] is the eigenvector corresponding to 
                the eigenvalue eigenvalues[i].
        
                
                rotate data with found eigenvectors
                Use Vt.conj().T @ Vt to get a square form of the eigenvectors
                in order to get the original matrix form
                """
                if( self.debug):
                    print( 'shape of FrqSeries: {}'.format( np.shape( FrqSeries)))
                    print( 'shape of VtNeg: {}'.format( np.shape( VtNeg)))
                Inner = PCAPrep@ VtNeg.conj().T
                if( self.debug):
                    print( 'shape of Inner: {}'.format( np.shape( Inner)))
                    
                """
                reshape the PCA rotated matrix to original ndim = 1 array form
                """
                #reshapedFrqSeries = np.zeros( np.shape( OrgFrqSeries), dtype = complex)
                #tempArray = (Inner)@ VtNeg
                #Maxf = np.max( np.abs( Artfaxis))
                #SqrtLen = np.max( np.shape( tempArray))
                #for colidx in np.arange( 0, SqrtLen):
                #    o = 0
                #    IntvFx = self.fmin* float( 0 + colidx + 1)
                #    while( ( o < SqrtLen) & ( IntvFx <= Maxf)):
                #        IntvFx = self.fmin* float( colidx * SqrtLen + o )
                #        #ctrIdx = np.argmin( np.abs( myAx - IntvFx))# - difflen
                #        
                #        #win = sg.windows.exponential( actlen, ctrIdx, sym = False)
                #        #plt.plot( win)
                #        #plt.show()
                #        #input()
                #        
                #        
                #        #reshapedFrqSeries[o, colidx] = reshapedFrqSeries[o, colidx] + FrqSeries[self.posind][nonzerocond][ rowidx] ### if I add up the columns which have already been filled
                #        #reshapedFrqSeries[o, colidx] = FrqSeries[nonzeroind][ rowidx]
                #        #reshapedFrqSeries[colidx, o] = np.sum( FrqSeries* win)/ np.sum( win)
                #        #reshapedFrqSeries[colidx, o] = FrqSeries[ ctrIdx]
                #        reshapedFrqSeries[ colidx * SqrtLen + o] = tempArray[colidx, o]
                #        
                #        #Nfaxis
                #        
                #        
                #        o = o + 1
                #        #LstO = o
                
                #PCAFrqSeriesNeg = reshapedFrqSeries
                #del reshapedFrqSeries
                """
                reshape the PCA rotated matrix to original ndim = 1 array form done
                """
                PCAFrqSeriesNeg = ( (Inner)@ VtNeg).reshape( 1, ApproxSerLen)
                
                #ApproxSerLen = 2* ApproxSerLen - 1 # cause only the positive freq
                # content was changed
                #FrqSeries = ((FrqSeries)).reshape( 1, ApproxSerLen)
                
                
                """
                end
                
                
                reduce to original length of frequency-series and stack negative
                and positive frequencies together
                """
                FrqSeries = FrqSeries[0:bakactlen]
                
                
                PCAFrqSeries = PCAFrqSeries[0, :]#0:bakactlen]
                PCAFrqSeriesNeg = PCAFrqSeriesNeg[0, :]#0:bakactlen]
                #PCAFrqSeries = np.hstack( ( PCAFrqSeries[::-1], PCAFrqSeries))[0:bakactlen]
                PCAFrqSeries = np.hstack( ( PCAFrqSeriesNeg, PCAFrqSeries))
                ApproxSerLen = len( PCAFrqSeries)
                PCAFrqSeries = PCAFrqSeries[0:bakactlen]
                
                
                
                #print( 'shape of PCAFrqSeries: {}'.format( np.shape( PCAFrqSeries)))
                #print( 'ApproxSerLen: {}'.format( ApproxSerLen))
                
                if( self.debug):
                    plt.close()
                    plt.plot( self.faxis, np.abs( FrqSeries), alpha = 0.3, label = 'FrqSeries')
                    plt.plot( self.faxis, np.abs( PCAFrqSeries), alpha = 0.3, label = 'PCAFrqSeries')
                    plt.legend( loc = 'best')
                    plt.grid( which = 'both')
                    plt.show()
                    sys.exit()
                if( self.debug):
                    print( 'PCAFrqSeries: {}'.format( PCAFrqSeries))



                
                if( self.debug):
                    plt.close()
                    #posind = self.posind[ np.argwhere( self.posind < ApproxSerLen)]
                    plt.loglog( self.faxis[self.posind], np.abs( FrqSeries[self.posind]), alpha = 0.3, label = 'FrqSeries pos')
                    plt.loglog( self.faxis[self.posind], np.abs( PCAFrqSeries[self.posind]), alpha = 0.3, label = 'PCAFrqSeries pos')
                    #plt.loglog( self.faxis[self.posind], np.abs( PCAFrqSeries), alpha = 0.3, label = 'PCAFrqSeries')
                    plt.loglog( self.faxis[self.posind], np.abs( OrgFrqSeries[self.posind]), alpha = 0.2, label = 'OrgFrqSeries pos')
                    plt.grid( which = 'both')
                    plt.legend( loc = 'best')
                    plt.show()
                    plt.close()
                    #posind = self.posind[ np.argwhere( self.posind < ApproxSerLen)]
                    plt.loglog( np.abs( self.faxis[self.negind]), np.abs( FrqSeries[self.negind]), alpha = 0.3, label = 'FrqSeries neg')
                    plt.loglog( np.abs( self.faxis[self.negind]), np.abs( PCAFrqSeries[self.negind]), alpha = 0.3, label = 'PCAFrqSeries neg')
                    #plt.loglog( self.faxis[self.posind], np.abs( PCAFrqSeries), alpha = 0.3, label = 'PCAFrqSeries')
                    plt.loglog( np.abs( self.faxis[self.negind]), np.abs( OrgFrqSeries[self.negind]), alpha = 0.2, label = 'OrgFrqSeries neg')
                    plt.grid( which = 'both')
                    plt.legend( loc = 'best')
                    plt.show()
                    #sys.exit()
                AllPCAFrqSeries.append( PCAFrqSeries.flatten())
                if( Maxind[0] < len(indPos)):
                    Maxvecs[0] = vecsPos
                    Maxvals[0] = valsPos
                    MaxVt[0] = VtPos
                    MaxApproxSerLen = ApproxSerLen
                    Maxind[0] = max( Maxind[0], len(indPos))
                if( Maxind[1] < len(indNeg)):
                    Maxvecs[1] = vecsNeg
                    Maxvals[1] = valsNeg
                    MaxVt[1] = VtNeg
                    MaxApproxSerLen = ApproxSerLen
                    Maxind[1] = max( Maxind[1], len(indNeg))
            #sys.exit()
            PCAFrqSeries = np.array( AllPCAFrqSeries).reshape( bakShape)
            #Maxind = max( Maxind, len(ind))
        if( self.axis == 0):
            for FrqSeries in BakFrqSeries.T:
               
                OrgFrqSeries = np.copy( np.abs( FrqSeries))
                
                actlen = len( OrgFrqSeries)
                bakactlen = np.copy( actlen)
                """
                artifically extend the frequency-series, so that one can derive
                a NxN-matrix of it ... extension will be removed at the end
                
                Zitat: Google gemini: 
                    https://share.google/aimode/Q1AZ0bTVj4VEFfX6z
                Während die PCA im Zeitbereich empfindlich auf 
                Phasenverschiebungen reagiert (ein verschobenes Signal wirkt 
                wie ein anderes Muster), ist die FFT (bzw. das 
                Amplitudenspektrum) phaseninvariant. Die PCA auf der FFT 
                findet daher: Wiederkehrende Akkorde oder Klangfarben: Wenn 
                Ihr Signal ein System ist, das in verschiedenen Fenstern immer 
                wieder dieselbe Kombination von Frequenzen erzeugt (z.B. ein 
                Grundton mit bestimmten Oberschwingungen), erkennt die PCA 
                diese Kombination als einen einzigen Eigenvektor. Spektrale 
                Texturen: Sie finden Muster, die über das gesamte Frequenzband 
                verteilt sind, aber immer gemeinsam auftreten (z.B. das 
                spezifische Frequenzprofil eines Maschinenschadens).
                
                """
                
                
                """
                positive frequencies
                """
                
                indPos, vecsPos, valsPos, VtPos, PCAPrep, ApproxSerLen = self.ArtExtendSpec( FrqSeries[self.posind])
                
                if( self.debug):
                    #print( 'covmat is: {}'.format( covmat))
                    #print( 'shape of covmat is: {}'.format( np.shape( covmat)))
                    #print( 'shape of vals is: {}'.format( np.shape( vals)))
                    print( 'shape of vecsPos is: {}'.format( np.shape( vecsPos)))
                
                
                
                
            
                """
                eigenvalues(…, M) array ... vals
        
                The eigenvalues, each repeated according to its multiplicity. 
                The eigenvalues are ordered due to SVD usage. The resulting 
                array will be of complex type. When a is real the resulting 
                eigenvalues will be real (0 imaginary part) or occur in 
                conjugate pairs.
        
                eigenvectors(…, M, M) array ... Vt
        
                The normalized (unit “length”) eigenvectors, such that the 
                column eigenvectors[i, :] is the eigenvector corresponding to 
                the eigenvalue eigenvalues[i].
        
                
                rotate data with found eigenvectors
                Use Vt.conj().T @ Vt to get a square form of the eigenvectors
                in order to get the original matrix form
                """
                if( self.debug):
                    print( 'shape of FrqSeries: {}'.format( np.shape( FrqSeries)))
                    print( 'shape of VtPos: {}'.format( np.shape( VtPos)))
                Inner = PCAPrep@ VtPos.conj().T
                if( self.debug):
                    print( 'shape of Inner: {}'.format( np.shape( Inner)))
                PCAFrqSeries = ( (Inner)@ VtPos).reshape( 1, ApproxSerLen)
                
                """
                negative frequencies
                """
                
                indNeg, vecsNeg, valsNeg, VtNeg, PCAPrep, ApproxSerLen = self.ArtExtendSpec( FrqSeries[self.negind])
                
                if( self.debug):
                    #print( 'covmat is: {}'.format( covmat))
                    #print( 'shape of covmat is: {}'.format( np.shape( covmat)))
                    #print( 'shape of vals is: {}'.format( np.shape( vals)))
                    print( 'shape of vecsNeg is: {}'.format( np.shape( vecsNeg)))
                
                
                
                
            
                """
                eigenvalues(…, M) array ... vals
        
                The eigenvalues, each repeated according to its multiplicity. 
                The eigenvalues are ordered due to SVD usage. The resulting 
                array will be of complex type. When a is real the resulting 
                eigenvalues will be real (0 imaginary part) or occur in 
                conjugate pairs.
        
                eigenvectors(…, M, M) array ... Vt
        
                The normalized (unit “length”) eigenvectors, such that the 
                column eigenvectors[i, :] is the eigenvector corresponding to 
                the eigenvalue eigenvalues[i].
        
                
                rotate data with found eigenvectors
                Use Vt.conj().T @ Vt to get a square form of the eigenvectors
                in order to get the original matrix form
                """
                if( self.debug):
                    print( 'shape of FrqSeries: {}'.format( np.shape( FrqSeries)))
                    print( 'shape of VtNeg: {}'.format( np.shape( VtNeg)))
                Inner = PCAPrep@ VtNeg.conj().T
                if( self.debug):
                    print( 'shape of Inner: {}'.format( np.shape( Inner)))
                PCAFrqSeriesNeg = ( (Inner)@ VtNeg).reshape( 1, ApproxSerLen)
                
                #ApproxSerLen = 2* ApproxSerLen - 1 # cause only the positive freq
                # content was changed
                #FrqSeries = ((FrqSeries)).reshape( 1, ApproxSerLen)
                
                
                """
                end
                
                
                reduce to original length of frequency-series and stack negative
                and positive frequencies together
                """
                FrqSeries = FrqSeries[0:bakactlen]
                
                
                PCAFrqSeries = PCAFrqSeries[0, :]#0:bakactlen]
                PCAFrqSeriesNeg = PCAFrqSeriesNeg[0, :]#0:bakactlen]
                #PCAFrqSeries = np.hstack( ( PCAFrqSeries[::-1], PCAFrqSeries))[0:bakactlen]
                PCAFrqSeries = np.hstack( ( PCAFrqSeriesNeg, PCAFrqSeries))
                ApproxSerLen = len( PCAFrqSeries)
                PCAFrqSeries = PCAFrqSeries[0:bakactlen]
                
                
                if( self.debug):
                    plt.close()
                    plt.plot( self.faxis, np.abs( FrqSeries), alpha = 0.3, label = 'FrqSeries')
                    plt.plot( self.faxis, np.abs( PCAFrqSeries), alpha = 0.3, label = 'PCAFrqSeries')
                    plt.legend( loc = 'best')
                    plt.grid( which = 'both')
                    plt.show()
                    sys.exit()
                if( self.debug):
                    print( 'PCAFrqSeries: {}'.format( PCAFrqSeries))



                
                if( self.debug):
                    plt.close()
                    #posind = self.posind[ np.argwhere( self.posind < ApproxSerLen)]
                    plt.loglog( self.faxis[self.posind], np.abs( FrqSeries[self.posind]), alpha = 0.3, label = 'FrqSeries pos')
                    plt.loglog( self.faxis[self.posind], np.abs( PCAFrqSeries[self.posind]), alpha = 0.3, label = 'PCAFrqSeries pos')
                    #plt.loglog( self.faxis[self.posind], np.abs( PCAFrqSeries), alpha = 0.3, label = 'PCAFrqSeries')
                    plt.loglog( self.faxis[self.posind], np.abs( OrgFrqSeries[self.posind]), alpha = 0.2, label = 'OrgFrqSeries pos')
                    plt.grid( which = 'both')
                    plt.legend( loc = 'best')
                    plt.show()
                    plt.close()
                    #posind = self.posind[ np.argwhere( self.posind < ApproxSerLen)]
                    plt.loglog( np.abs( self.faxis[self.negind]), np.abs( FrqSeries[self.negind]), alpha = 0.3, label = 'FrqSeries neg')
                    plt.loglog( np.abs( self.faxis[self.negind]), np.abs( PCAFrqSeries[self.negind]), alpha = 0.3, label = 'PCAFrqSeries neg')
                    #plt.loglog( self.faxis[self.posind], np.abs( PCAFrqSeries), alpha = 0.3, label = 'PCAFrqSeries')
                    plt.loglog( np.abs( self.faxis[self.negind]), np.abs( OrgFrqSeries[self.negind]), alpha = 0.2, label = 'OrgFrqSeries neg')
                    plt.grid( which = 'both')
                    plt.legend( loc = 'best')
                    plt.show()
                    #sys.exit()
                AllPCAFrqSeries.append( PCAFrqSeries.flatten())
                if( Maxind[0] < len(indPos)):
                    Maxvecs[0] = vecsPos
                    Maxvals[0] = valsPos
                    MaxVt[0] = VtPos
                    MaxApproxSerLen = ApproxSerLen
                    Maxind[0] = max( Maxind[0], len(indPos))
                if( Maxind[1] < len(indNeg)):
                    Maxvecs[1] = vecsNeg
                    Maxvals[1] = valsNeg
                    MaxVt[1] = VtNeg
                    MaxApproxSerLen = ApproxSerLen
                    Maxind[1] = max( Maxind[1], len(indNeg))
                
            #sys.exit()
            PCAFrqSeries = np.array( AllPCAFrqSeries).reshape( bakShape)
            #Maxind = max( Maxind, len(ind))



        #sys.exit()
        return PCAFrqSeries, Maxind, Maxvecs, Maxvals, MaxVt, MaxApproxSerLen
    
    ###
    # from gemini ...
    # create a wrapper function around the minimum_scalar function in order
    # to prevent repetition of the same integer value in the minimum_scalar
    # attempt
    ###
    def starte_optimierung(self, nInit, aktuelles_RefSpec):
        verlauf = {}  # Dictionary to remember seen integers: {n_integer: result}

        def target_wrapper(n_float):
            n_int = int(round(n_float))
            
            # If we haven't calculated this integer yet, do it now
            if n_int not in verlauf:
                verlauf[n_int] = self.__SlepTest__(n_int, aktuelles_RefSpec)
                
            # Return the stored result (instantly skips calculation on duplicates)
            return verlauf[n_int]
    
        nOpt = minimize_scalar( target_wrapper, bounds = (1, nInit), method = 'bounded')
        print( 'nOpt.x derived to: {}, nOpt.success derived to: {}, nOpt.message derived to: {}, nOpt.fun derived to: {}'.format( nOpt.x, nOpt.success, nOpt.message, nOpt.fun))
        return int( round(nOpt.x))
    ###
    # end
    ###
    
    
    def __SlepProd__( self, n, RefSpec):
        
        
        
        n = int( np.ceil( n))
        """
        Iterative approach for Dyn-Slep-derivation
        """
        
        
        #dump = self.augdata
        
        
        dump = RefSpec
        if( dump.ndim == 1):
            dump = np.atleast_2d( dump)
        #if( np.shape( dump)[0] == np.shape( self.data)[1] or np.shape( dump)[1] == np.shape( self.data)[0]):
        #    self.augdata = dump.T
        #    dump = self.augdata
        if( self.debug):
            print( 'shape of dump: {}'.format( np.shape( dump)))
        """
        
        derive frequency axis cause field-augmentation has to be concerned
        
        """
        
        self.len = np.shape( self.augt)[0]
        #SqrtLen = int( np.floor( np.sqrt( self.len)))
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.fmin = 1.0/ float( self.len)/ self.dt
        self.__faxis__()
        
        N, M = np.shape( dump)
        
        if( self.axis == 0):
            M = 1
        elif( self.axis == 1):
            N = 1
        
        if( self.debug):
            print( 'Length for dpss: {}'.format( [N, M][self.axis]))
            print( 'N, M', N, M)
        #slep = dpss( [N, M][self.axis], 5, self.NumOfSlep, norm = 2)
        ### defined bandwidth of signals to be averaged as a factor to be  multiplied to BW
        ### For example a W = 24 with a sample length of 86400 seconds that you average over
        ### one hour of samples along that one day
        W = 5        
        BW = 2.* self.fmin* W 
        twoNW = float( self.len* self.dt* BW) # from scipy doc(https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.windows.dpss.html): Standardized half bandwidth corresponding to 2*NW = BW/f0 = BW*N*dt where dt is taken as 1
        if( self.debug):#self.debug):
            print( 'twoNW', twoNW)
        
        
        fdata = np.zeros( ( N, M), dtype = complex)
        if( self.debug):
            print( '\nActual number of slepian windows to be derived: {}'.format( n))
        if( self.debug):
            print( 'shape of prepared fdata: {}'.format( np.shape( fdata)))
        
        
        bakslep, ratios = dpss( [N, M][self.axis], twoNW, n, norm = 'approximate', return_ratios = True)
        #bakslep, ratios = dpss( [N, M][self.axis], twoNW, 200, norm = 'approximate', return_ratios = True)
        
        
        
        if( self.debug):
            print( 'length of ratios: {}'.format( len( ratios)))
            print( 'ratios: {}'.format( ratios))
        
        if( self.axis == 0):
            for k, d in enumerate( dump.T):
                if( self.debug):
                    print( 'shape of d in dump is: {}'.format( np.shape( d)))
                if( bakslep.ndim == 1):
                    slep = np.atleast_2d( bakslep[:,-1]).T # taking only last slepian window for covariance tests
                else:
                    slep = bakslep
                #print( 'shape of d is: {}'.format( np.shape( d)))
                #print( 'shape of slep is: {}'.format( np.shape( slep)))
                #print( 'shape of d* slep is: {}'.format( np.shape( d* slep)))
                #print( 'mean of d[{}] with nans is: {}'.format( k, np.mean( d)))
                #print( 'isnan of d in dump is: {}'.format( np.isnan( d)))
                if( np.any( np.isnan( d))):
                    #print( "[{}]-th column has some nan's".format( k, d))
                    raise self.MyException( "[{}]-th column has some nan's".format( k, d))
                if( self.debug):
                    print( 'shape of d is: {}'.format( np.shape( d)))
                    print( 'shape of slep is: {}'.format( np.shape( slep)))
                phase = fft.fft( d)
                phase = phase/ np.abs( phase)
                dummy = ( d* slep).T
                if( self.debug):
                    print( 'shape of d* slep is: {}'.format( np.shape( dummy)))
                #fdata[k,:] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis) * np.sqrt( self.fmin)), axis = 1)
                #fdata[:, k] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis)), axis = 1)
                """
                 * self.AmplitudeScale ... to care for different length of timeseries
                 * self.slepScale ... to care for Nyquist scaling in slepian averaging due to power-spectral-density average and taking square-root
                """
                Unweighed = np.nanmedian( np.abs( fft.fft( dummy* self.AmplitudeScale, axis = self.axis))**2.0, axis = 1)* self.slepScale
                #SlepSum = np.nansum( np.abs( fft.fft( slep, axis = self.axis)), axis = 1)
                fdata[:, k] = np.sqrt( Unweighed)* np.exp( 1j* phase)
            #print( 'shape of fData in MySlepianApproach is:\n\t{}'.format( np.shape( fData)))
            #sys.exit()
        elif( self.axis == 1):
            for k, d in enumerate( dump):
                #slep = dpss( [N, M][self.axis], twoNW, n, norm = 'approximate')
                if( self.debug):
                    print( 'shape of d in dump is: {}'.format( np.shape( d)))
                if( bakslep.ndim == 1):
                    slep = np.atleast_2d( bakslep[-1,:])
                else:
                    slep = bakslep
                    #slep = np.ones( np.shape( bakslep), dtype = float) + np.random.sample( np.shape( bakslep))* 0.5 + 1j* np.zeros( np.shape( bakslep), dtype = float)
                    
                    #slep = slep/ np.atleast_2d( np.sum( slep, axis = 0))
                #print( 'shape of d is: {}'.format( np.shape( d)))
                #print( 'shape of slep is: {}'.format( np.shape( slep)))
                #print( 'shape of d* slep is: {}'.format( np.shape( d* slep)))
                #print( 'mean of d[{}] with nans is: {}'.format( k, np.mean( d)))
                #print( 'isnan of d in dump is: {}'.format( np.isnan( d)))
                if( np.any( np.isnan( d))):
                    #print( "[{}]-th column has some nan's".format( k, d))
                    raise self.MyException( "[{}]-th column has some nan's with d: {}".format( k, d))
                if( self.debug):
                    print( 'shape of d is: {}'.format( np.shape( d)))
                    print( 'shape of slep is: {}'.format( np.shape( slep)))
                phase = fft.fft( d)
                phase = phase/ np.abs( phase)
                dummy = ( d* slep)
                if( self.debug):
                    print( 'shape of d* slep is: {}'.format( np.shape( dummy)))
                #fdata[k,:] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis) * np.sqrt( self.fmin)), axis = 1)
                #fdata[k, :] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis)), axis = 0).T
                """
                 * self.AmplitudeScale ... to care for different length of timeseries
                 * self.slepScale ... to care for Nyquist scaling in slepian averaging due to power-spectral-density average and taking square-root
                """
                UnWeighed = np.nanmedian( np.abs( fft.fft( dummy* self.AmplitudeScale, axis = self.axis))**2.0, axis = 0).T* self.slepScale
                #SlepSum = np.nansum( np.abs( fft.fft( slep, axis = self.axis)), axis = 0).T
                fdata[k, :] = np.sqrt( UnWeighed)* np.exp( 1j* phase)
            #print( 'shape of fData in MySlepianApproach is:\n\t{}'.format( np.shape( fData)))
            #sys.exit()
        #dummy = np.abs( fft.fftshift( fdata, axes = self.axis))
        dummy = fft.fftshift( fdata, axes = self.axis)
        #dummy = np.abs( fdata)
        if( False):
            ndummy = []
            
            phase = dummy/ np.abs( dummy)
            abszissa = np.log10( np.abs( dummy))
            ordinate = np.log10( np.abs( RefSpec))
            if( ordinate.ndim == 1):
                """
                transposing to be in same shape as 
                """
                ordinate = np.atleast_2d( ordinate)
                if( np.argmin( ordinate) != np.argmin( abszissa)):
                    ordinate = ordinate.T
                    ordinate = np.repeat( ordinate, np.min( np.shape( abszissa))).reshape( np.shape( abszissa))
            
            if( self.debug):
                print( 'shape of ordinate after equalization of shapes: {}'.format( np.shape( ordinate)))
                print( 'shape of abszissa after equalization of shapes: {}'.format( np.shape( abszissa)))
            """
            derive a calibrated version of the slepian tapered spectrum
            """
            for k, ( el, des) in enumerate( zip( abszissa[:, self.posind], ordinate[:, self.posind])):
                poly = np.polyfit( el[n:], des[n:], 1)
                helpv = 10.0**( np.polyval( poly, abszissa[ k,:]))
                
                helpv = helpv* np.exp( 1j* phase[k,:])
                ndummy.append( helpv)
                
            if( self.debug):
                print( 'shape of ndummy: {}'.format( np.shape( ndummy)))
            ndummy = np.array( ndummy, dtype = complex)
            
            """
            end
            """
            
            if( self.debug):
                if( RefSpec.ndim == 1):
                    """
                    transposing to be in same shape as 
                    """
                    ordinate = np.atleast_2d( RefSpec)
                    if( np.argmin( ordinate) != np.argmin( abszissa)):
                        ordinate = ordinate.T
                        ordinate = np.repeat( ordinate, np.min( np.shape( abszissa))).reshape( np.shape( abszissa))
                for bl, el, r in zip( ndummy, dummy, ordinate):

                    plt.close()
                    plt.loglog( self.faxis[self.posind], np.abs( el)[self.posind], label = 'dummy', alpha = 0.3)
                    plt.loglog( self.faxis[self.posind], np.abs( r)[self.posind], label = 'reference', alpha = 0.3)
                    plt.loglog( self.faxis[self.posind], np.abs( bl)[self.posind], label = 'ndummy', alpha = 0.3)
                    plt.grid( which = 'both')
                    plt.legend( loc = 'best')
                    plt.show()
                #sys.exit()
            """
            replace the slepian spectrum with its calibrated counterpart
            """
            dummy = ndummy
            del ndummy
            
            """
            end
            """
        if( self.debug):
            print( 'dummy:\n{}'.format( dummy))
            print( 'Is there any nan in dummy:\n{}'.format( np.any( np.isnan( dummy))))
            print( 'shape of dummy after normalization with dummymag: {}'.format( np.shape( dummy)))
        
        
        
        return dummy, ratios
    
    
    
    def EstSpec( self, alphak, SingleTaperPeriodgram):
        
        if( self.debug):
            print( 'starting EstSpec...')
            
            print( 'Input Variables are:...\nalphak: {}\nshape of SingleTaperPeriodgram: {}'.format( alphak, np.shape( SingleTaperPeriodgram)))
        """
        alphak ... are the weights being derived by EstWeigh, they are derived 
        from the eigenvalues from dpss ( ratios)
        """
        TaperedPeriodogramm = np.zeros( np.shape( SingleTaperPeriodgram), dtype = complex)
        if( False):
            alphakLen = len( alphak)
            
            alphak = np.hstack( ( np.zeros( ( len( SingleTaperPeriodgram) - alphakLen)), alphak[::-1]))
        for k, el in enumerate( alphak):
            TaperedPeriodogramm[k] = el* SingleTaperPeriodgram[k]
        if( self.debug):
            print( 'EstSpec...application of weights done...')
        UnWeigh = TaperedPeriodogramm#np.sqrt( np.abs( TaperedPeriodogramm* self.AmplitudeScale)**2.0)* self.slepScale#np.nansum( np.abs( TaperedPeriodogramm))
        #print( 'shape of UnWeigh: {}'.format( np.shape( UnWeigh)))
        #sys.exit()
        #Weigh = np.nansum( np.abs( alphak))
        Weigh = np.sqrt( np.nansum( np.abs( alphak)**2.0))
        EstSpec = UnWeigh/ Weigh
        return EstSpec
    
    
    def EstWeigh( self, EigVal, SingleTaperPeriodgram, MeanError, ApproxSerLen):
        #self.debug = True
        
        #print( 'shape of EigVal: {}'.format( np.shape( EigVal)))
        #print( 'shape of EigVal[1]: {}'.format( np.shape( EigVal[1])))
        #EigVal = np.hstack( ( EigVal[0], EigVal[1]))
        
        if( self.debug):
            print( 'starting EstWeigh...')
            
            print( 'Input Variables are:...\nEigval: {}\nshape of SingleTaperPeriodgram: {}\nMeanError: {}'.format( EigVal, np.shape( SingleTaperPeriodgram), MeanError))
            #print( 'shape of EigVal: {}'.format( np.shape( EigVal)))
            print( 'len EigVal: {}'.format( len( EigVal)))
            print( 'ApproxSerLen: {}'.format( ApproxSerLen))
        
        TaperedVarSq = np.zeros( ( np.max( np.shape( SingleTaperPeriodgram)), ), dtype = complex)
        TaperedVar = np.zeros( ( np.max( np.shape( SingleTaperPeriodgram)) , ), dtype = complex)
        OneMinEigvalMeanErr = np.zeros( ( np.max( np.shape( SingleTaperPeriodgram)) , ), dtype = complex)
        if( self.debug):
            print( 'shape of TaperedVarSq: {}'.format( np.shape( TaperedVarSq)))
            print( 'shape of TaperedVar: {}'.format( np.shape( TaperedVar)))
            print( 'shape of SingleTaperPeriodgram: {}'.format( np.shape( SingleTaperPeriodgram)))
        #FlatEigVal = EigVal
        #print( 'SingleTaperPeriodgram: {}'.format( SingleTaperPeriodgram))
        #cond = np.isfinite( SingleTaperPeriodgram.flatten())
        for k, el in enumerate( EigVal):
            if( self.debug):
                print( 'EigVal[{}]: {}'.format( k, el))
            
            TaperedVarSq = TaperedVarSq + el* SingleTaperPeriodgram**2.0
            #print( 'TaperedVarSq: {}'.format( k, TaperedVarSq))
            TaperedVar = TaperedVar + el* SingleTaperPeriodgram
            #print( 'TaperedVar: {}'.format( k, TaperedVar))
            OneMinEigvalMeanErr = OneMinEigvalMeanErr + (1.0 - el)* MeanError
        
        TaperedVarSq = TaperedVarSq.reshape( np.shape( SingleTaperPeriodgram))
        TaperedVar = TaperedVar.reshape( np.shape( SingleTaperPeriodgram))
        
        
        if( self.debug):
            print( 'shape of TaperedVarSq: {}'.format( np.shape( TaperedVarSq)))
            print( 'shape of TaperedVar: {}'.format( np.shape( TaperedVar)))
        """
        extend EigVal with zeros in order to ensure the cutting according to
        the length of ApproxSerLen
        """
        #print( 'length of FlatEigVal: {}, ApproxSerLen: {}'.format( len( FlatEigVal), ApproxSerLen))
        #print( 'int( np.sqrt( ApproxSerLen))', int( np.sqrt( ApproxSerLen)))
        #print( '( int( np.sqrt( ApproxSerLen)))**2.0', ( int( np.sqrt( ApproxSerLen)))**2.0)
        #EigVal = np.hstack( ( EigVal, np.zeros( ( int( np.sqrt( ApproxSerLen)) - len( EigVal)))))
        
        #FlatEigVal = np.hstack( ( EigVal, np.zeros( ( ApproxSerLen) - len( EigVal) )) )
        
        #print( 'length of FlatEigVal: {}, ApproxSerLen: {}'.format( len( FlatEigVal), ApproxSerLen))
        #FlatEigVal = np.diag( FlatEigVal).reshape( ApproxSerLen)[:len( SingleTaperPeriodgram)]
        #if( self.debug):#self.debug):
        #    print( 'shape of FlatEigVal: {}'.format( np.shape( FlatEigVal)))
        
        if( self.debug):
            print( 'shape of OneMinEigvalMeanErr: {}'.format( np.shape( OneMinEigvalMeanErr)))
        enumerator = TaperedVarSq
        denominator = ( TaperedVar + OneMinEigvalMeanErr)**2.0
        if( self.debug):
            print( 'nansum of enumerator in EigWeigh: {}'.format( np.nansum( enumerator)))
            print( 'nansum of denominator in EigWeigh: {}'.format( np.nansum( denominator)))
        EstWeigh = np.abs( enumerator/ denominator)#* float( len( enumerator))
        
        
        """
        normalize weights in order to always minimize that sample with the highest weight
        """
        cond = np.isnan( EstWeigh)
        ind = np.argwhere( cond).flatten()
        EstWeigh[:, ind] = 0.0       
        EstWeigh = EstWeigh#/ np.nansum( np.abs( EstWeigh))#np.max( np.abs( EstWeigh))
        #EstWeigh = fft.ifft( fft.ifftshift( EstWeigh))
        #EstWeigh = EstWeigh* 0.3
        if( self.debug):
            print( 'EstWeigh after normalization: {}'.format( EstWeigh))
            #input()
        #self.debug = False
        return EstWeigh
    
    
    
    def ThomsonMultitaperAdj( self, SingleTaperPeriodgram):
        
        if( self.debug):
            print( 'starting ThomsonMultitaperAdj...')
            
            print( 'Input Variables are:...\nshape of SingleTaperPeriodgram: {}'.format( np.shape( SingleTaperPeriodgram)))
        #sc = 0.1
        InitWeigh = np.ones( (len( SingleTaperPeriodgram), ), dtype = complex)
        EstWChg = np.ones( (len( SingleTaperPeriodgram), ), dtype = float)*0.5 + ( np.random.sample( len( SingleTaperPeriodgram))*0.5) - 0.5#1e-2
        EstW = InitWeigh# - ( np.abs( np.random.sample( len( InitWeigh))* sc - 0.5)*0.2 + 1j* np.abs( np.random.sample( len( InitWeigh))* sc - 0.5)*0.2)
        #print( 'EstW: {}'.format( EstW))
        #EstSp = self.EstSpec( EstW, SingleTaperPeriodgram)
        
        actNumOfSlep = 2
        EstSp, ratios = self.__SlepProd__( actNumOfSlep, fft.ifft( fft.ifftshift( SingleTaperPeriodgram)) )
        
        if( self.debug):
            """
            Just for debugging 
            """
            
            print( 'self.augdata: {}'.format( self.augdata))
            print( ' fft.ifft( fft.ifftshift( SingleTaperPeriodgram)): {}'.format( fft.ifft( fft.ifftshift( SingleTaperPeriodgram))))
            El = fft.ifftshift( SingleTaperPeriodgram)
            print( 'El: {}'.format( El))
            El = fft.ifft( El).real#/ sg.windows.hann( len( SingleTaperPeriodgram), sym = False)
            print( 'np.shape( El)', np.shape( El))
            print( 'np.shape( self.augdata)', np.shape( self.augdata))
            
            
            El = np.stack( ( El,) * np.min( np.shape( self.augdata)), axis=0)
            El = El.reshape( np.shape( self.augdata))
            print( 'np.shape( El)', np.shape( El))
            #print( 'El: {}'.format( El))
            #sys.exit()
            
            #sys.exit()
            for k, ( el, rl) in enumerate( zip( El, self.augdata)):
                print( 'el: {}'.format( el))
                print( 'np.isnan( el): {}'.format( np.isnan( el)))
                print( 'rl: {}'.format( rl))
                print( 'np.isnan( rl): {}'.format( np.isnan( rl)))
                #print( 'np.shape( el)', np.shape( el))
                plt.plot( el/ np.std( el), alpha = 0.3, label = 'SingleTaperPeriodgram timeseries {} normalized'.format( k))
                plt.plot( rl/ np.std( rl), alpha = 0.3, label = 'self.augdata {} normalized'.format( k))
                plt.legend( loc = 'best')
                plt.grid( which = 'both')
                plt.show()
            sys.exit()     
            """
            end
            """
        N, M = np.shape( np.atleast_2d( SingleTaperPeriodgram))
        
        if( self.axis == 0):
            M = 1
        elif( self.axis == 1):
            N = 1
        
        if( self.debug):
            print( 'Length for dpss: {}'.format( [N, M][self.axis]))
            print( 'N, M', N, M)
        W = 5        
        BW = 2.* self.fmin* W 
        twoNW = float( self.len* self.dt* BW) # from scipy doc(https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.windows.dpss.html): Standardized half bandwidth corresponding to 2*NW = BW/f0 = BW*N*dt where dt is taken as 1
        if( self.debug):#self.debug):
            print( 'twoNW', twoNW)
        EstSp, ratios = self.__SlepProd__( actNumOfSlep, fft.ifft( fft.ifftshift( SingleTaperPeriodgram)) )
        #_, ratios = dpss( [N, M][self.axis], twoNW, actNumOfSlep, norm = 'approximate', return_ratios = True)
        normedRatios = ratios/ np.max( ratios)
        lgRatios = np.log10( normedRatios)
        
        """
        keeping only those eigenvalues which at least contribute 10 percent
        """
        cond = lgRatios >= -1.0
        ratios = ratios[ np.argwhere( cond).flatten()]
        oldratios = ratios[:-1]
        if( self.debug):
            print( 'ratios: {}'.format( ratios))
        if( self.debug):
            plt.close()
            plt.plot( self.faxis[self.posind], np.abs( SingleTaperPeriodgram)[self.posind], alpha = 0.2, label = 'np.abs( SingleTaperPeriodgram)[self.posind]')
            plt.plot( self.faxis[self.posind], np.abs( EstSp.flatten())[self.posind], alpha = 0.2, label = 'np.abs( EstSp)[self.posind]')
            plt.legend( loc = 'best')
            plt.show()
        #sys.exit()
        #ind = np.argwhere( EstSp).flatten()
        if( self.debug):
            print( 'EstSpec returned EstSp with shape: {}'.format( np.shape( EstSp)))
        
        
        MnErr = np.abs( np.var( fft.ifft( fft.ifftshift( EstSp))))# - fft.ifft( fft.ifftshift( SingleTaperPeriodgram)) ))
        if( True):#self.debug):
            print( 'Derived initial MnErr to: {}'.format( MnErr))
        OldErr = 3.0* MnErr# + 10.0**(19.0)
        ErrorZero = OldErr
        OldOldErr = OldErr + 1.0
        
        k = 0
        SgnWeigh = np.sign( InitWeigh)
        OnePercCond = MnErr > ErrorZero* 1e-2
        
        
        
        """
        initialize poly
        """
        poly = np.array( [-1.0, 0.0], dtype = float)
        bakpoly = poly
        #poly = np.polyfit( np.arange( 0, 3), np.array( [0.0, 1.0, 2.0]), deg = 1)
        #print( 'poly: {}'.format( poly))
        #input()
        """
        check if percentage of errorchange drops below level ....ErrPercChg??
        """
        ErrPercChg = np.abs( MnErr - OldErr)/ OldErr *1.1
        OldErrPercChg = ErrPercChg + 1e-9
        
        
        #( ErrPercChg < OldErrPercChg) & 
        #while( ( OldErr > MnErr) & OnePercCond & ( ErrPercChg > 1e-9) & ( poly[1] < 0) & ( len( oldratios) < len( ratios)) | ( k < 3)):
        #
        while( ( OnePercCond & ( np.abs(OldErrPercChg - ErrPercChg) > 1e-3) & ( len( oldratios) < len( ratios)) & ( MnErr < OldOldErr)) | ( k < 3)): #  & ( np.sign( poly[0]) == np.sign( bakpoly[0]))
            if( self.debug):
                print( 'Iteration {}...'.format( k))
            bakEstSpec = np.copy( EstSp)
            bakEstW = np.copy( EstW)
            bakpoly = np.copy( poly)
            OldErrPercChg = ErrPercChg
            oldratios = ratios
            ErrPercChg = np.abs( MnErr - OldErr)/ OldErr
            if( self.debug):
                print( 'OldErrPercChg: {} - ErrPercChg: {}'.format( OldErrPercChg, ErrPercChg))
            k = k + 1
            actNumOfSlep = k + 1
            if( k >=2 ):
                errorArr = np.array( [ OldOldErr, OldErr, MnErr], dtype = float)
                #poly = np.polyfit( np.arange( 0, 3), errorArr, deg = 2)
                poly = np.polyfit( np.arange( 0, 3), errorArr, deg = 1)
                if( self.debug):
                    print( 'poly: {}'.format( poly))
                #input()
                
            if( OldErr > MnErr):
                baklenofInd = len( ratios) + 1
            else:
                OldErr > MnErr
            OldOldErr = OldErr
            OldErr = MnErr
            
            if( np.all( EstWChg == (0.0 +1j* 0.0))):
                EstW = EstW
            else:
                #EstW = EstW + 2.0* ( SgnWeigh* EstWChg)/ 3.0
                WChg = SgnWeigh* EstWChg/ 3.0#* np.sign( poly[0])
                if( self.debug):
                    print( 'SgnWeigh* EstWChg: {}'.format( WChg))
                EstW = EstW + WChg
                #EstW = EstWChg
            
            #print( 'shape of SingleTaperPeriodgram', np.shape( SingleTaperPeriodgram))
            #print( 'shape of EstSp', np.shape( EstSp))
            #EstSp = self.EstSpec( EstW, SingleTaperPeriodgram)
            #EstSp = self.EstSpec( EstW, EstSp.flatten())
            #EstSp, ratios = self.__SlepProd__( actNumOfSlep, fft.ifft( fft.ifftshift( EstSp.flatten())) )
            EstSp, ratios = self.__SlepProd__( actNumOfSlep, fft.ifft( fft.ifftshift( SingleTaperPeriodgram)) )
            
            #SgnWeigh = np.sign( np.abs( SingleTaperPeriodgram) - np.abs( EstSp))* np.sign( poly[0])
            SgnWeigh = np.sign( bakpoly[0])* np.sign( poly[0])* np.sign( np.abs( SingleTaperPeriodgram) - np.abs( EstSp))#np.ones( ( len( SingleTaperPeriodgram), ), dtype = float)
            #SgnWeigh = -np.sign( OldErr - MnErr)* SgnWeigh
            
            #SgnWeigh = -np.sign( np.abs( bakEstSpec) - np.abs( EstSp))
            MnErr = np.abs( np.var( fft.ifft( fft.ifftshift( EstSp))))# - fft.ifft( fft.ifftshift( bakEstSpec)) ))
            if( self.debug):
                print( 'Derived new MnErr to: {}'.format( MnErr))
            
            #bakactlen = len( SingleTaperPeriodgram)
            
            if( self.debug):
                plt.close()
                plt.loglog( self.faxis[self.posind], np.abs( SingleTaperPeriodgram)[self.posind], alpha = 0.2, label = 'np.abs( SingleTaperPeriodgram)[self.posind]')
                plt.loglog( self.faxis[self.posind], np.abs( EstSp.flatten())[self.posind], alpha = 0.2, label = 'np.abs( EstSp)[self.posind] [{}]'.format( k))

                #plt.loglog( self.faxis[self.posind], np.abs( SingleTaperPeriodgram - EstSp.flatten())[self.posind], alpha = 0.2, label = 'np.abs( SingleTaperPeriodgram)[self.posind] - np.abs( EstSp)[self.posind] [{}]'.format( k))
                
                
                plt.legend( loc = 'best')
                plt.show()
            
            
            
            
            """
            end
            
            
            reduce to original length of frequency-series
            """
            
            #EstSp = EstSp[0, 0:bakactlen]
            
            
            #tapered, ratios = self.__SlepProd__( actNumOfSlep, fft.ifft( fft.ifftshift( EstSp)) )
            #_, ratios = dpss( [N, M][self.axis], twoNW, actNumOfSlep, norm = 'approximate', return_ratios = True)
            #print( 'ratios: {}'.format( ratios))
            normedRatios = ratios/ np.max( ratios)
            lgRatios = np.log10( normedRatios)
            
            """
            keeping only those eigenvalues which at least contribute 10 percent
            """
            cond = lgRatios >= -1.0
            ratios = ratios[ np.argwhere( cond).flatten()]
            if( self.debug):
                print( 'len of ratios: {}'.format( len(ratios)))
            #print( 'ratios: {}'.format( ratios.astype( float)))
            #tapered = tapered.flatten()
            #PCAFrqSeries = PCAFrqSeries[0, 0:bakactlen]
            if( self.debug):
                print( 'EspSp: {}'.format( EstSp))
                print( 'ratios: {}'.format( ratios))
                #print( 'shape of tapered: {}'.format( np.shape( tapered)))
            
            #print( 'shape of tapered: {}'.format( np.shape( tapered)))
            #print( 'shape of EstSp: {}'.format( np.shape( EstSp)))
            #sys.exit()
            #EstWChg = self.EstWeigh( ratios, tapered, MnErr, len( SingleTaperPeriodgram)).flatten()#[:len( SingleTaperPeriodgram)]
            EstWChg = self.EstWeigh( ratios, np.atleast_2d( EstSp), MnErr, len( SingleTaperPeriodgram)).flatten()#[:len( SingleTaperPeriodgram)]
            if( self.debug):
                print( 'EstWChg: {}'.format( EstWChg))
            #EstWChg = 2.0* EstWChg/ 3.0
            
            if( self.debug):
                print( 'OldErr > MnErr', OldErr > MnErr)
                print( 'OldErr', OldErr)
                print( 'MnErr', MnErr)
                print( '\n\nOnePercCond', OnePercCond)
                print( '( np.abs(OldErrPercChg - ErrPercChg) > 1e-3', np.abs(OldErrPercChg - ErrPercChg) > 1e-3)
                print( 'np.sign( poly[0]) == np.sign( bakpoly[0])', np.sign( poly[0]) == np.sign( bakpoly[0]))
                #print( 'len( oldratios) < len( ratios)', len( oldratios) < len( ratios))
                print( 'k < 3', k < 3)
            #input()
        """
        revert to old ... and therefore best found solution
        """
        EstW = bakEstW
        EstSp = bakEstSpec
        ind = oldratios
        
        
        
        if( self.debug):
            print( 'shape of EstWChg: {}'.format( np.shape( EstWChg)))
            print( 'shape of SingleTaperPeriodgram: {}'.format( np.shape( SingleTaperPeriodgram)))
        
        
        NumOfEigVals = len( ind)
        print( 'final amount of slepians derived: {}'.format( NumOfEigVals))
        #EstimateSpec = SingleTaperPeriodgram
        
        EstimateSpec = EstSp#EstimateSpec[ 0,:]
        tapered, _ = self.__SlepProd__( NumOfEigVals, fft.ifft( fft.ifftshift( SingleTaperPeriodgram)) )
        #tapered, _ = self.__SlepProd__( NumOfEigVals, fft.ifft( fft.ifftshift( EstimateSpec)) )
        """
        Average two samples to remove ringing behaviour in spectra
        """
        EstimateSpec = tapered
        out = EstimateSpec.flatten() + np.hstack( ( EstimateSpec.flatten()[1:], EstimateSpec.flatten()[-1]))#tapered.flatten()
        out = out/2.0
        """
        end
        """
        if( self.debug):
            plt.close()
            plt.loglog( self.faxis[self.posind], np.abs( SingleTaperPeriodgram)[self.posind], alpha = 0.2, label = 'np.abs( SingleTaperPeriodgram)[self.posind]')
            plt.loglog( self.faxis[self.posind], np.abs( out)[self.posind], alpha = 0.2, label = 'out[self.posind] [{}]'.format( NumOfEigVals))
            #plt.loglog( self.faxis[self.posind], np.abs( tapered.flatten())[self.posind], alpha = 0.2, label = 'tapered[self.posind] [{}]'.format( NumOfEigVals))

            #plt.loglog( self.faxis[self.posind], np.abs( SingleTaperPeriodgram - EstSp.flatten())[self.posind], alpha = 0.2, label = 'np.abs( SingleTaperPeriodgram)[self.posind] - np.abs( EstSp)[self.posind] [{}]'.format( k))
            
            plt.grid( which = 'both')
            plt.legend( loc = 'best')
            plt.show()
        #!!! there is a different spectra at the end of that call, when compared to the original spectra ... check if the same spectra a plotted
        """
        end ThomsonMultitaperAdj
        """
        return NumOfEigVals, out
    
    
    def __MySlepianApproachMag__( self):
        
        from scipy.signal.windows import dpss
        #self.debug = True
        #fmin = 1.0/ np.max( np.shape( self.data))/ self.dt
        
        if( self.despike):
            print( 'Despiking data...')
            self.DeSpike()
            self.data, changeind = self.despdata, self.newchgind
            if( self.debug):
                plt.close()
                plt.plot( self.t, self.data.T, alpha = 0.2, label = 'self.data after DeSpike routine')
                plt.show()
                #sys.exit()
        
        if( self.debug):
            print( 'shape of self.t is:\t{}'.format( np.shape( self.t)))
        
        if( self.aug):
            """ if there is some field augmentation applied"""
            #self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.FIELDAUGMENT( self.t, self.data)
            self.avg = np.atleast_2d( np.nanmean( self.data, axis = self.axis)).T
            if( self.debug):
                print( 'self.axis', self.axis)
                print( 'np.shape( self.avg)', np.shape( self.avg))
                print( 'np.shape( self.data)', np.shape( self.data))
                for el in self.data:
                    print( 'el in self.data is: {}'.format( el))
                
                #sys.exit()
            if( self.axis == 0):
                self.FIELDAUGMENT( self.t, self.data - self.avg)
            if( self.axis == 1):
                #self.FIELDAUGMENT( self.t, self.data - self.avg.T)
                self.FIELDAUGMENT( self.t, self.data - self.avg)
                #self.augdata = self.augdata.T
            
        else:
            """ if there is no field augmentation applied"""
            self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.t, self.data, 0, self.shape[self.axis] - 1
        
        if( self.debug):
            print( 'shape of self.t is:\t{}'.format( np.shape( self.t)))
            print( 'shape of self.augt is:\t{}'.format( np.shape( self.augt)))
            print( 'np.shape( self.data)', np.shape( self.data))
            print( 'shape of self.augdata is:\t{}'.format( np.shape( self.augdata)))
            print( 'self.orgStartInd is:\t{}'.format( self.orgStartInd))
            print( 'self.orgEndInd is:\t{}'.format( self.orgEndInd))
        dump = self.augdata
        #if( np.shape( dump)[0] == np.shape( self.data)[1] or np.shape( dump)[1] == np.shape( self.data)[0]):
        #    self.augdata = dump.T
        #    dump = self.augdata
        if( self.debug):
            print( 'shape of dump: {}'.format( np.shape( dump)))
        """
        
        derive frequency axis cause field-augmentation has to be concerned
        
        """
        
        self.len = np.shape( self.augt)[0]
        SqrtLen = int( np.floor( np.sqrt( self.len)))
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.fmin = 1.0/ float( self.len)/ self.dt
        self.__faxis__()
        
        
        N, M = np.shape( dump)
        
        if( self.debug):
            print( 'Length for dpss: {}'.format( [N, M][self.axis]))
            print( 'N, M', N, M)
        #slep = dpss( [N, M][self.axis], 5, self.NumOfSlep, norm = 2)
        ### defined bandwidth of signals to be averaged as a factor to be  multiplied to BW
        ### For example a W = 24 with a sample length of 86400 seconds that you average over
        ### one hour of samples along that one day
        W = 5        
        BW = 2.* self.fmin* W 
        twoNW = float( self.len* self.dt* BW) # from scipy doc(https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.windows.dpss.html): Standardized half bandwidth corresponding to 2*NW = BW/f0 = BW*N*dt where dt is taken as 1
        if( self.debug):#self.debug):
            print( 'twoNW', twoNW)
            #sys.exit()
        
        Initfdata = fft.fftshift( fft.fft( self.augdata, axis = self.axis))
        if( self.debug):
            slep = np.nanmean( dpss( [N, M][self.axis], twoNW, self.NumOfSlep, norm = 2), axis = 0)
            print( 'ThomsonMultitaperAdj attempt...')
            a, b = self.ThomsonMultitaperAdj( Initfdata[0,:]* slep)
            print( 'Number of Slepian derived: {}\nEstSpectrum: {}'.format( a, b))
            print( 'ThomsonMultitaperAdj attempt done')
        
        if( self.debug):
            if( self.axis == 0):
                plt.loglog( self.faxis[self.posind], np.abs( Initfdata[self.posind, :]), alpha = 0.3, label = 'initfdata')
            elif( self.axis == 1):
                plt.loglog( self.faxis[self.posind], np.abs( Initfdata[:, self.posind]).T, alpha = 0.3, label = 'initfdata')
                #plt.loglog( self.faxis[self.posind], np.abs( Initfdata[0, self.posind]).T, alpha = 0.3, label = 'initfdata')
                #plt.loglog( self.faxis[self.posind], np.abs( b[self.posind]).T, alpha = 0.3, label = 'Estimated spectrum')
        #if( self.debug and self.axis == 1):
        #    print( 'shape of self.fdataMag is:\t{}'.format( np.shape( self.fdataMag)))
        #    print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
        #    plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[:, self.posind].T))
        #    #plt.plot( self.faxis, np.abs( self.fdataMag))
        if( self.debug):
            plt.grid( which = 'both')
            plt.legend( loc = 'best')
            plt.show()
            sys.exit()
        if( self.debug):
            print( 'shape of inital fdata: {}'.format( np.shape( Initfdata)))
            #sys.exit()

        #######
        # Trying to dynamically derive optimal amount of slepian windows according to "When Slepian Meets Fiedler: Putting a Focus on the Graph Spectrum - Van De Ville, Demesmaeker, Preti, 2017 IEEE Paper"
        #######
        #self.debug = True
        if( self.DynNumOfSlep):
            
            #sys.exit()
            if( self.debug):
                print( np.shape( self.data))
            
            print( 'Testing for best amount of slepian windows...')
            
            
            AllNOpt, AllEstSpec = [], []
            RefSpectra = fft.fftshift( fft.fft( self.augdata, axis = self.axis), axes = self.axis)
            for el in RefSpectra:
                
                nOpt, EstSpec = self.ThomsonMultitaperAdj( el)#* slep)
                AllNOpt.append( nOpt)
                AllEstSpec.append( EstSpec)
            
            if( self.debug):
                print( 'self.axis', self.axis)
                #print( 'reference:\n{}'.format( reference))
                #print( 'shape of reference:\n{}'.format( np.shape( reference)))
            if( self.debug):
                plt.close()
                for k, ( al, bl, nTap) in enumerate( zip( RefSpectra, AllEstSpec, AllNOpt)):
                    #plt.loglog( self.faxis[self.posind], np.abs( el)[self.posind], alpha = 0.3, label = 'adjusted slepian windowed spec {}'.format( k))
                    plt.loglog( self.faxis[self.posind], np.abs( bl)[self.posind], alpha = 0.3, label = 'EstSpec pos {} with {} slepian tapers'.format( k, nTap))
                    plt.loglog( self.faxis[self.posind], np.abs( al)[self.posind], alpha = 0.3, label = 'RefSpectra pos {}'.format( k))
                    plt.legend( loc= 'best')
                    plt.grid( which = 'both')
                    plt.show()
                for k, ( al, bl, nTap) in enumerate( zip( RefSpectra, AllEstSpec, AllNOpt)):
                    #plt.loglog( self.faxis[self.posind], np.abs( el)[self.posind], alpha = 0.3, label = 'adjusted slepian windowed spec {}'.format( k))
                    plt.loglog( np.abs( self.faxis[self.negind]), np.abs( bl)[self.negind], alpha = 0.3, label = 'EstSpec neg {} with {} slepian tapers'.format( k, nTap))
                    plt.loglog( np.abs( self.faxis[self.negind]), np.abs( al)[self.negind], alpha = 0.3, label = 'RefSpectra neg {}'.format( k))
                    plt.legend( loc= 'best')
                    plt.grid( which = 'both')
                    plt.show()
                #sys.exit()
            
            
                    
                
            #condition = mymat > bound
            #print( condition)
            #sys.exit()
            #oldn = np.max( [oldn, newn])#len(eigvals)#* 2
            self.NumOfSlep = np.max( AllNOpt)
            
            print( 'Dynamically derived number of slepian windows: {}'.format( self.NumOfSlep))
            #sys.exit()
            self.debug = False
            if( self.NumOfSlep == 0):
                self.NumOfSlep = self.NumOfSlep + 1
            #print( 'self.dt', self.dt)
            #print( 'self.len', self.len)
            
        self.fdataMag = []
        for el in self.augdata:
            dummy, _ = self.__SlepProd__( self.NumOfSlep, el)
            self.fdataMag.append( dummy.flatten())
        self.fdataMag = np.array( self.fdataMag, dtype = complex).reshape( np.shape( self.augdata))
        
        #self.fdataMag = fdata.T
        
        if( self.debug):
            print( 'shape of self.fdataMag is:\t{}'.format( np.shape( self.fdataMag)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            if( self.axis == 0):
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[self.posind, :]), alpha = 0.2, label = 'self.fdataMag')
            elif( self.axis == 1):
                for k, ( al, bl, nTap) in enumerate( zip( RefSpectra, self.fdataMag, AllNOpt)):
                    plt.loglog( self.faxis[self.posind], np.abs( bl)[self.posind], alpha = 0.3, label = 'EstSpec pos {} with {} slepian tapers'.format( k, nTap))
                    plt.loglog( self.faxis[self.posind], np.abs( al)[self.posind], alpha = 0.3, label = 'RefSpectra pos {}'.format( k))
                    plt.legend( loc= 'best')
                    plt.grid( which = 'both')
                    plt.show()
            sys.exit()
        #if( self.debug and self.axis == 1):
        #    print( 'shape of self.fdataMag is:\t{}'.format( np.shape( self.fdataMag)))
        #    print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
        #    plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[:, self.posind].T))
        #    #plt.plot( self.faxis, np.abs( self.fdataMag))
        if( self.debug):
            plt.grid( which = 'both')
            plt.legend( loc = 'best')
            plt.show()
            sys.exit()
        
        return #self.faxis, self.fdataMag
    
    
    
    def __MySlepianApproachAng__( self):
        
        from scipy.signal.windows import dpss
        
        #fmin = 1.0/ np.max( np.shape( self.data))/ self.dt
        if( self.despike):
            print( 'Despiking data...')
            self.DeSpike()
            self.data, changeind = self.despdata, self.newchgind
            if( self.debug):
                plt.close()
                plt.plot( self.t, self.data.T, alpha = 0.2, label = 'self.data after DeSpike routine')
                plt.show()
                #sys.exit()
        if( self.debug):
            print( 'shape of self.t is:\t{}'.format( np.shape( self.t)))
        if( self.aug):
            """ if there is some field augmentation applied"""
            #self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.FIELDAUGMENT( self.t, self.data)
            self.avg = np.nanmean( self.data, axis = self.axis)
            if( self.axis == 0):
                self.FIELDAUGMENT( self.t, self.data - self.avg)
            if( self.axis == 1):
                self.FIELDAUGMENT( self.t, self.data - self.avg)
                self.augdata = self.augdata
            
        else:
            """ if there is no field augmentation applied"""
            self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.t, self.data, 0, self.shape[self.axis] - 1
        if( self.debug):
            print( 'shape of self.t is:\t{}'.format( np.shape( self.t)))
            print( 'shape of self.augt is:\t{}'.format( np.shape( self.augt)))
            print( 'shape of self.augdata is:\t{}'.format( np.shape( self.augdata)))
            print( 'self.orgStartInd is:\t{}'.format( self.orgStartInd))
            print( 'self.orgEndInd is:\t{}'.format( self.orgEndInd))
        dump = self.augdata
        #if( np.shape( dump)[0] == np.shape( self.data)[1] or np.shape( dump)[1] == np.shape( self.data)[0]):
        #    self.augdata = dump.T
        #    dump = self.augdata
        if( self.debug):
            print( 'shape of dump: {}'.format( np.shape( dump)))
        """
        
        derive frequency axis cause field-augmentation has to be concerned
        
        """
        
        self.len = np.shape( self.augt)[0]
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.fmin = 1.0/ float( self.len)/ self.dt
        self.__faxis__()
        if( False):
            if( self.PeriodAxis):
                #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
                self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
                self.posind = np.argwhere( self.faxis >= 1.0/ self.fnyq).flatten()#[0]
                self.negind = np.argwhere( self.faxis <= -1.0/ self.fnyq).flatten()#[0]
            else:
                #self.faxis = np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
                self.faxis =  np.arange( -self.fnyq, self.fnyq, self.fmin)
                self.posind = np.argwhere( self.faxis >= self.fmin).flatten()#[0]
                self.negind = np.argwhere( self.faxis <= self.fmin).flatten()#[0]
        N, M = np.shape( dump)
        if( self.debug):
            print( 'Length for dpss: {}'.format( [N, M][self.axis]))
        #slep = dpss( [N, M][self.axis], 5, self.NumOfSlep, norm = 2)
        BW = 2.0*self.fmin
        twoNW = int( self.len* self.dt* BW) # from scipy doc(https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.windows.dpss.html): Standardized half bandwidth corresponding to 2*NW = BW/f0 = BW*N*dt where dt is taken as 1
        if( self.debug):
            print( 'self.NumOfSlep', self.NumOfSlep)
        #sys.exit()
        slep = dpss( [N, M][self.axis], twoNW, self.NumOfSlep, norm = 'approximate')
        #if( self.axis == 1):
        #    slep = slep.T
        if( self.debug):
            print( 'shape of slep is: {}'.format( np.shape( slep)))
        fdata = np.zeros( ( N, M), dtype = complex)
        if( self.debug):
            classicfdata = np.copy( fdata)
        
        if( self.axis == 0):
            for k, d in enumerate( dump.T):
                #print( 'shape of d is: {}'.format( np.shape( d)))
                #print( 'shape of slep is: {}'.format( np.shape( slep)))
                #print( 'shape of d* slep is: {}'.format( np.shape( d* slep)))
                #print( 'mean of d[{}] with nans is: {}'.format( k, np.mean( d)))
                #print( 'isnan of d in dump is: {}'.format( np.isnan( d)))
                if( np.any( np.isnan( d))):
                    #print( "[{}]-th column has some nan's".format( k, d))
                    raise self.MyException( "[{}]-th column has some nan's".format( k, d))
                if( self.debug):
                    print( 'shape of d is: {}'.format( np.shape( d)))
                dummy = ( d* slep).T
                if( self.debug):
                    print( 'shape of d* slep is: {}'.format( np.shape( dummy)))
                #fdata[k,:] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis) * np.sqrt( self.fmin)), axis = 1)
                #fdata[:, k] = np.nanmean( np.angle( fft.fft( dummy, axis = self.axis)), axis = 1)
                pureFFT = fft.fft( dummy, axis = self.axis)
                fdata[:, k] = np.nanmean( pureFFT.real, axis = 1) + 1j* np.nanmean( pureFFT.imag, axis = 1)
                #print( 'mean of fdata[:, {}] with nans is: {}'.format( k, np.mean( fdata[:, k])))
                if( self.debug):
                    classicfdata[:,k] = fft.fft( d* sg.parzen( len(d)), axis = self.axis)
            #print( 'shape of fData in MySlepianApproach is:\n\t{}'.format( np.shape( fData)))
            #sys.exit()
        elif( self.axis == 1):
            for k, d in enumerate( dump):
                #print( 'shape of d is: {}'.format( np.shape( d)))
                #print( 'shape of slep is: {}'.format( np.shape( slep)))
                #print( 'shape of d* slep is: {}'.format( np.shape( d* slep)))
                #print( 'mean of d[{}] with nans is: {}'.format( k, np.mean( d)))
                #print( 'isnan of d in dump is: {}'.format( np.isnan( d)))
                if( np.any( np.isnan( d))):
                    #print( "[{}]-th column has some nan's".format( k, d))
                    raise self.MyException( "[{}]-th column has some nan's".format( k, d))
                if( self.debug):
                    print( 'shape of d is: {}'.format( np.shape( d)))
                dummy = ( d* slep)
                if( self.debug):
                    print( 'shape of d* slep is: {}'.format( np.shape( dummy)))
                #fdata[k,:] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis) * np.sqrt( self.fmin)), axis = 1)
                #fdata[k, :] = np.nanmean( np.angle( fft.fft( dummy, axis = self.axis)), axis = 0).T
                pureFFT = fft.fft( dummy, axis = self.axis)
                fdata[k, :] = ( np.nanmean( pureFFT.real, axis = 0) + 1j* np.nanmean( pureFFT.imag, axis = 0)).T
                if( self.debug):
                    classicfdata[k,:] = fft.fft( d* sg.parzen( len(d)), axis = self.axis).T
            #print( 'shape of fData in MySlepianApproach is:\n\t{}'.format( np.shape( fData)))
            #sys.exit()
        self.fdataAng = fft.fftshift( fdata, axes = self.axis)
        if( self.debug):
            classicfdata = fft.fftshift( classicfdata, axes = self.axis)
        #self.fdataMag = fdata.T
        if( self.debug):
            print( 'shape of self.fdataAng is:\t{}'.format( np.shape( self.fdataAng)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            if( self.axis == 0):
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataAng[self.posind, :]), label = 'Slepian-windowed')
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataAng.real[self.posind, :]), label = 'Slepian-windowed.real')
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataAng.imag[self.posind, :]), label = 'Slepian-windowed.imag')
                if( self.debug):
                    plt.loglog( self.faxis[self.posind], np.abs( classicfdata[self.posind, :]), alpha = 0.2, label = 'Parzens-windowed')
            elif( self.axis == 1):
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataAng[:, self.posind]).T, label = 'Slepian-windowed')
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataAng.real[:, self.posind]).T, label = 'Slepian-windowed.real')
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataAng.imag[:, self.posind]).T, label = 'Slepian-windowed.imag')
                if( self.debug):
                    plt.loglog( self.faxis[self.posind], np.abs( classicfdata[:, self.posind]), alpha = 0.2, label = 'Parzens-windowed')
        #if( self.debug and self.axis == 1):
        #    print( 'shape of self.fdataMag is:\t{}'.format( np.shape( self.fdataMag)))
        #    print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
        #    plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[:, self.posind].T))
        #    #plt.plot( self.faxis, np.abs( self.fdataMag))
        if( self.debug):
            plt.grid(True)
            plt.legend( loc='best')
            plt.show()
            #sys.exit()
        return #self.faxis, self.fdataMag
    
    
    
    def __myInvFFTProc__( self):
        #self.filtdata = fft.ifft( fft.ifftshift( self.filtfdata.T/ np.sqrt( self.fmin), axes = self.axis), axis = self.axis)
        #self.filtdata = fft.ifft( fft.ifftshift( self.filtfdata/ np.sqrt( self.fmin), axes = self.axis), axis = self.axis)
        mag = np.abs( self.filtfdata)/ self.AmplitudeScale
        ang = np.angle( self.filtfdata)
        self.filtfdata = mag* np.exp( 1j* ang)
        self.filtdata = fft.ifft( fft.ifftshift( self.filtfdata, axes = self.axis), axis = self.axis)
        if( np.argmax( np.shape( self.taper)) == np.argmax( np.shape( self.filtdata))):
            self.filtdata = self.filtdata/ ( self.taper + 1.0)
        else:
            self.filtdata = self.filtdata/ ( self.taper.T + 1.0)
        return
    
    
    def __expfilt__( self):
        
        #from scipy.signal.windows import dpss
        
        #fmin = 1.0/ np.max( np.shape( self.data))/ self.dt
        """
        self.ce ... central frequency of exponential window or central period
            of exponential window depending on the frequency/ period axis used
        self.de ... damping decrement value for formula taken from 
            D = np.exp( de)
            tauval = float( N)/2.0 * 8.69/ D
            from:
                https://en.wikipedia.org/wiki/Window_function
                from Exponential or Poisson window
                N...length of window
                tau...damping decrement in dB
                center...index of maxval of window
                window = scipy.signal.exponential(
                        N, center = self.ctrind, tau = tauval
                        , sym = False)
        """
        self.__myFFTproc__()
        if( self.debug):
            #n = 0
            for al, el in zip( self.augdata.T, self.data.T):
                plt.plot( self.augt, al, 'r')
                plt.plot( self.t, el - np.nanmean( el), 'g')
            
            plt.show()
        """
        
        derive frequency axis cause field-augmentation has to be concerned
        
        """
        self.len = np.shape( self.augt)[0]
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.fmin = 1.0/ float( self.len)/ self.dt
        #self.AmplitudeScale = np.sqrt( self.fmin* self.dt**2.0)
        self.__faxis__()

        ########
        # pos frequencies
        ########
        if( self.debug):
            print( 'self.ce is :\t{}'.format( self.ce))
        self.ctrind = np.argmin( np.abs( self.faxis - self.ce))
        if( self.debug):
            print( 'self.ctrind is:\t{}'.format( self.ctrind))
        if( self.dim > 1):
            N, M = np.shape( self.augdata)
        else:
            N = np.shape( self.augdata)
        if( False):
            w = np.hstack( ( np.zeros( ( self.ctrind - 1, ) ), 1.0, np.zeros( ( N - self.ctrind,))))
        D = np.exp( self.de) # dezibel decay over half of window length
        #w = sg.gaussian( upperind -lowerind, std = 3.0* np.nanstd( np.abs( np.nanmean( fData[:,lowerind:upperind:], axis = 0))))
        #w = np.power( sg.parzen( upperind -lowerind), 4.0)
        tauval = float( N)/2.0 * 8.69/ D # https://en.wikipedia.org/wiki/Window_function from Exponential or Poisson window
        #w = sg.exponential( upperind -lowerind, center = np.argmax( np.abs( fData[:,lowerind:upperind:])), tau = tauval, sym = False) # exponential window function https://en.wikipedia.org/wiki/Window_function
        w = ( sg.exponential( N, center = self.ctrind, tau = tauval, sym = False)) # exponential window function https://en.wikipedia.org/wiki/Window_function
        ########
        # neg frequencies
        ########
        self.ctrind = np.argmin( np.abs( self.faxis + self.ce))
        if( self.debug):
            print( 'self.ctrind is:\t{}'.format( self.ctrind))
        #if( not debug):
        w = w + ( sg.exponential( N, center = self.ctrind, tau = tauval, sym = False)) # exponential window function https://en.wikipedia.org/wiki/Window_function
        w = w/ np.nanmax( w)
        if( self.kind == 'block'):
            w = np.ones( np.shape( w)) - w
        elif( self.kind == 'pass'):
            pass
        else:
            print( '\n\n\nWrong input for "kind"-option...stopping')
            return
        #sys.exit()
        if( False):
            w = w + np.hstack( ( np.zeros( ( self.ctrind - 1, ) ), 1.0, np.zeros( ( N - self.ctrind,))))
        self.filter = w
        if( self.debug):
            print( 'shape of self.filter is:\t{}'.format( np.shape( self.filter)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            #plt.semilogx( self.faxis[self.posind], self.fdataAng[self.posind, :])
            plt.plot( self.faxis, np.abs( self.filter))
            plt.grid(True)
            plt.show()
        #self.__myFFTproc__()
        if( self.debug):
            print( 'shape of self.filter is: {}'.format( np.shape( self.filter)))
            print( 'shape of self.fdata is: {}'.format( np.shape( self.fdata)))
        #scale = self.AmplitudeScale#np.sqrt( self.fmin* self.dt**2.0)
        absfdata = np.abs( self.fdata)# will be done in __myInvFFTProc__ /self.AmplitudeScale#/ scale
        angfdata = np.angle( self.fdata)
        
        if( self.dim > 1):
            self.filtfdata = ( absfdata * np.atleast_2d( self.filter).T) * np.exp( 1j* angfdata)
        else:
            self.filtfdata = absfdata * self.filter * np.exp( 1j* angfdata)
        
        #self.fdataMag = fdata.T
        if( self.debug):
            print( 'shape of self.filtfdata is:\t{}'.format( np.shape( self.filtfdata)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            #plt.semilogx( self.faxis[self.posind], self.fdataAng[self.posind, :])
            #plt.plot( self.faxis, np.abs( self.filtfdata))
            plt.loglog( self.faxis[self.posind], np.abs( absfdata[self.posind,:]), 'g', alpha = 0.2)
            plt.loglog( self.faxis[self.posind], np.abs( self.filtfdata[self.posind,:]), 'r', alpha = 0.2)
            plt.loglog( self.faxis[self.posind], np.abs( self.filter[self.posind]), 'b', alpha = 0.1)
            plt.grid(True)
            plt.show()
        #self.filtdata = np.real( fft.ifft( fft.ifftshift( self.filtfdata, axes = self.axis), axis = self.axis))#[self.orgStartInd:self.orgEndInd, :]
        self.__myInvFFTProc__()
        #self.filtdata = np.real( fft.ifft( self.filtfdata))[self.orgStartInd:self.orgEndInd, :]
        if( self.debug):
            plt.plot( self.augt[self.orgStartInd:self.orgEndInd], self.data.T, 'g', alpha = 0.2)
            plt.plot( self.augt[self.orgStartInd:self.orgEndInd], self.filtdata[self.orgStartInd:self.orgEndInd], 'r', alpha = 0.2)
            plt.grid(True)
            plt.show()
            #sys.exit()
        
        
        
        return self.augt[self.orgStartInd:self.orgEndInd], self.filtdata
    
    
    
    def __lbubfilt__( self):
        
        #from scipy.signal.windows import dpss
        
        #fmin = 1.0/ np.max( np.shape( self.data))/ self.dt
        #self.debug = True
        self.__myFFTproc__()
        if( self.debug):
            #n = 0
            for al, el in zip( self.augdata.T, self.data.T):
                plt.plot( self.augt, al, 'r')
                plt.plot( self.t, el - np.nanmean( el), 'g')
            
            plt.show()
        """
        
        derive frequency axis cause field-augmentation has to be concerned
        
        """
        
        #print( 'self.fmin before augment', self.fmin)
        self.len = np.shape( self.augt)[self.axis]
        #self.fnyq = 1.0/ 2.0/ self.dt # will be derived in self.__faxis__()
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        #self.fmin = 1.0/ float( self.len)/ self.dt
        self.__faxis__()
        self.fmin = np.min( self.faxis[self.posind]) # changed due to problems 
        #with small arrays. fmin was wrong then, cause artificial shift to
        #ensure equal samples at positive and negative spectrum
        #print( 'self.fmin after augment', self.fmin)
        #print( 'np.min( np.abs( self.faxis))', np.min( np.abs( self.faxis)))
        #print( 'np.max( np.abs( self.faxis))', np.max( np.abs( self.faxis)))
        #sys.exit()
        #self.fmin = 1.0/ float( self.len)/ self.dt
        if( self.debug):
            print( 'self.PeriodAxis is :\t{}'.format( self.PeriodAxis))
            print( 'self.lb is :\t{}'.format( self.lb))
            print( 'self.ub is :\t{}'.format( self.ub))
            print( 'self.faxis', self.faxis)
            print( 'np.max( np.abs( self.faxis))', np.max( np.abs( self.faxis)))
            #for f in self.faxis:
            #    print( f)
            #sys.exit()
        
        
        
        
        if( self.debug):
            sortind = np.argwhere( self.faxis > 0.0).flatten()
            
            print( 'shape of self.faxis[sortind] is:\t{}'.format( np.shape( self.faxis[sortind])))
            plt.semilogx( np.arange( 0, len( sortind)), self.faxis[sortind], '+')
            #plt.plot( self.faxis, w)
            plt.grid(True)
            plt.show()
            sys.exit()
        """
        # pos frequencies
        """
        if( not self.PeriodAxis):
            self.lbind = np.argmin( np.abs( self.faxis[self.posind] - self.lb))
            self.ubind = np.argmin( np.abs( self.faxis[self.posind] - self.ub))
        else:
            self.lbind = np.argmin( np.abs( self.faxis[self.posind] - self.lb))
            self.ubind = np.argmin( np.abs( self.faxis[self.posind] - self.ub))            
        if( self.debug):
            print( 'self.lbind is:\t{}'.format( self.lbind))
            print( 'self.ubind is:\t{}'.format( self.ubind))
            print( 'self.lb is :\t{}'.format( self.lb))
            print( 'self.ub is :\t{}'.format( self.ub))
            sys.exit()
        
        if( False):
            if( self.dim > 1):
                N, M = np.shape( self.augdata)
            else:
                N = np.shape( self.augdata)
        else:
            if( self.dim > 1):
                N, M = len( self.faxis[self.posind]), np.shape( self.augdata)[1]
            else:
                N = len( self.faxis[self.posind])
        D = np.exp( self.de) # dezibel decay over half of window length
        #w = sg.gaussian( upperind -lowerind, std = 3.0* np.nanstd( np.abs( np.nanmean( fData[:,lowerind:upperind:], axis = 0))))
        #w = np.power( sg.parzen( upperind -lowerind), 4.0)
        tauval = float( N)/2.0 * 8.69/ D # https://en.wikipedia.org/wiki/Window_function from Exponential or Poisson window
        #w = sg.exponential( upperind -lowerind, center = np.argmax( np.abs( fData[:,lowerind:upperind:])), tau = tauval, sym = False) # exponential window function https://en.wikipedia.org/wiki/Window_function
        if( debug):
            print( 'N', N)
            if( self.dim > 1):
                print( 'M', M)
            print( 'self.ub', self.ub)
            print( 'self.ubind', self.ubind)
            print( 'self.lb', self.lb)
            print( 'self.lbind', self.lbind)
            #sys.exit()
        rectlen = np.abs( self.ubind - self.lbind)
        """
        Upper Slope of exponential window
        """
        """
        Derive the index of self.faxis from the sample closest to 0 Hz/ seconds
        """
        if( not self.PeriodAxis):
            ZeroClosestInd = np.argmin( np.abs( self.faxis[self.posind] - self.fnyq))
            if( self.debug):
                print( 'self.fnyq', self.fnyq)
                print( 'ZeroClosestInd', ZeroClosestInd)
        else:
            ZeroClosestInd = np.argmin( np.abs( self.faxis[self.posind] - self.fmin))
            if( self.debug):
                print( 'self.fmin', self.fmin)
                print( 'ZeroClosestInd', ZeroClosestInd)

        """
        Define an array to determine which index is further away from ZeroClosestInd
        """
        windbnds = np.array( [ self.lbind, self.ubind]).flatten()
        
        """
        Find the index which is further away from ZeroClosestInd
        """
        if( not self.PeriodAxis):
            uprbndInd = np.argmin( np.abs( windbnds - ZeroClosestInd))
        else:
            uprbndInd = np.argmax( np.abs( windbnds - ZeroClosestInd))
        if( self.debug):
            print( 'uprbndInd', uprbndInd)
        
        """
        Use this cloest index to derive an exp-window with its peak at this closest index to zero Hertz
        """
        if( self.debug):
            print( 'N', N)
        w = sg.exponential( N, center = windbnds[uprbndInd], tau = tauval, sym = False) # exponential window function https://en.wikipedia.org/wiki/Window_function
        
        #wmaxind = np.argmax( np.abs( w))
        #w = ( sg.exponential( N, center = self.ubind, tau = tauval, sym = False)) # exponential window function https://en.wikipedia.org/wiki/Window_function
        wmaxind = np.argmax( np.abs( w))
        if( self.debug):
            print( 'wmaxind upslope', wmaxind)
        if( self.debug):
            #
            #plt.ion()
            w = w/ w[wmaxind]
            #sortind = np.argsort( self.faxis).flatten()
            #sortind = np.argwhere( self.faxis > 0.0).flatten()
            print( 'shape of w is:\t{}'.format( np.shape( w)))
            print( 'self.faxis[self.posind] is:\t{}'.format( self.faxis[self.posind]))
            print( 'shape of self.faxis[self.posind] is:\t{}'.format( np.shape( self.faxis[self.posind])))
            plt.loglog( self.faxis[self.posind], w)
            #plt.plot( self.faxis, w)
            plt.grid(True)
            plt.show()
            #plt.pause(0.1)
            #plt.clf()
            #plt.ioff()
            #sys.exit()
            """
            !!! self.faxis is starting for PeriodAxis== True  with SI=0.1s from -0.2 decreasing to -200000, jumping to +20000 and decreasing to 0.2 
            
            !!! take care during derivation of exponential window
            """
        
        
        """
        take care if PeriodAxis is used
        """
        if( not self.PeriodAxis):
            selind = np.arange( wmaxind, len( w))
            #selind = np.arange( 0, wmaxind - rectlen)
            
            upslope = w[selind]
            if( self.debug):
                print( 'self.PeriodAxis; indices of w are {}'.format( selind))
                print( 'np.shape( upslope) is:\t{}'.format( np.shape( upslope)))
                sys.exit()
        else:
            #selind = np.arange( 0, wmaxind)
            #selind = np.arange( wmaxind, len( w) - rectlen)
            selind = np.arange( 0, wmaxind)#wmaxind, len( w) - rectlen)
            upslope = w[selind] 
            if( self.debug):
                print( 'indices of upslope are {}'.format( selind))
                print( 'np.shape( upslope) is:\t{}'.format( np.shape( upslope)))
        if( self.debug):
            #
            #plt.ion()
            
            #sortind = np.argsort( self.faxis).flatten()
            #sortind = np.argwhere( self.faxis > 0.0).flatten()
            print( 'shape of upslope is:\t{}'.format( np.shape( upslope)))
            print( 'self.faxis[self.posind][selind] is:\t{}'.format( self.faxis[self.posind][selind]))
            print( 'shape of self.faxis[self.posind][selind] is:\t{}'.format( np.shape( self.faxis[self.posind][selind])))
            plt.loglog( self.faxis[self.posind][selind], upslope)
            #plt.plot( self.faxis, w)
            plt.grid(True)
            plt.show()
            #plt.pause(0.1)
            #plt.clf()
            #plt.ioff()
            #sys.exit()
        posselind = np.copy( selind)
        if( self.debug):
            #w = w/ w[wmaxind]
            sortind = np.argsort( self.faxis[posselind]).flatten()
            print( 'shape of upslope is:\t{}'.format( np.shape( upslope)))
            print( 'shape of self.faxis[selind] is:\t{}'.format( np.shape( self.faxis[posselind])))
            plt.semilogx( self.faxis[self.posind][posselind], upslope, '+')
            #plt.plot( self.faxis[wmaxind:][sortind], upslope)
            plt.grid(True)
            plt.show()
            sys.exit()

        """
        rectangular window part take care of self.PeriodAxis == True
        """
        if( self.debug):
            print( 'self.ub', self.ub)
            print( 'self.ubind', self.ubind)
            print( 'self.lb', self.lb)
            print( 'self.lbind', self.lbind)
    
        if( np.abs( self.ubind - self.lbind) > 0):
            if( self.ubind >= self.lbind):
                #rect = np.ones( ( self.ubind - self.lbind - 1, ))
                rect = np.ones( ( self.ubind - self.lbind, ))
            else:
                #rect = np.ones( ( self.lbind - self.ubind - 1, ))
                rect = np.ones( ( self.lbind - self.ubind, ))
        else:
            rect = np.ones( ( 0, ))
        
        
        if( self.debug):
            print( 'self.lbind', self.lbind)
            print( 'self.ubind', self.ubind)
            print( 'shape of rect is:\t{}'.format( np.shape( rect)))
            
            
        """
        Lower slope of exponential window
        """
        """
        Derive the index of self.faxis from the sample closest to 0 Hz/ seconds
        """
        #ZeroClosestInd = np.argmin( np.abs( self.faxis - self.fmin))
        """
        Define an array to determine which index is closer to ZeroClosestInd
        """
        #windbnds = np.array( [ self.lbind, self.ubind]).flatten()
        """
        Find the index which is closer to ZeroClosestInd
        """
        #lwrbndInd = np.argmin( np.abs( windbnds - ZeroClosestInd))
        if( not self.PeriodAxis):
            lwrbndInd = np.argmax( np.abs( windbnds - ZeroClosestInd))
        else:
            lwrbndInd = np.argmin( np.abs( windbnds - ZeroClosestInd))
        if( self.debug):
            print( 'lwrbndInd', lwrbndInd)
            
        if( self.debug):
            #w = w/ w[wmaxind]
            #plt.loglog( self.faxis[self.posind], w, 'b', alpha = 0.6, label = 'lowerslope_window')
            #sortind = np.argwhere( self.faxis[selind]).flatten()
            
            
            plt.loglog( self.posind, self.faxis[self.posind], 'g', alpha = 0.4, label = 'whole axis')
            plt.loglog( self.posind[windbnds[ lwrbndInd]], self.faxis[self.posind][windbnds[ lwrbndInd]], '+b', alpha = 0.2, label = 'windbnds[ lwrbndInd]')
            plt.loglog( self.posind[windbnds[ uprbndInd]], self.faxis[self.posind][windbnds[ uprbndInd]], 'xr', alpha = 0.2, label = 'windbnds[ uprbndInd]')
            #print( 'shape of upslope is:\t{}'.format( np.shape( upslope)))
            #print( 'self.faxis[self.posind][selind] is:\t{}'.format( self.faxis[self.posind][selind]))
            #print( 'shape of self.faxis[self.posind][selind] is:\t{}'.format( np.shape( self.faxis[self.posind][selind])))
            #plt.loglog( self.faxis[self.posind][selind], upslope)
            #plt.plot( self.faxis, w)
            #plt.grid(True)
            #plt.show()
            #plt.plot( self.faxis[wmaxind:][sortind], upslope)
            plt.grid(True)
            plt.legend( loc = 'best')
            plt.show()
        """
        Use this cloest index to derive an exp-window with its peak at this closest index to zero Hertz
        """
        w = sg.exponential( N, center = windbnds[lwrbndInd], tau = tauval, sym = False) # exponential window function https://en.wikipedia.org/wiki/Window_function
        
        wmaxind = np.argmax( np.abs( w))
        if( self.debug):
            print( 'wmaxind lwslope', wmaxind)
        if( self.debug):
            print( 'self.ub', self.ub)
            print( 'self.ubind', self.ubind)
            print( 'self.lb', self.lb)
            print( 'self.lbind', self.lbind)
            print( 'windbnds[uprbndInd]', windbnds[uprbndInd])
            print( 'windbnds[lwrbndInd]', windbnds[lwrbndInd])
            if( self.debug):
                #walt = ( sg.exponential( N, center = windbnds[uprbndInd], tau = tauval, sym = False)) # exponential window function https://en.wikipedia.org/wiki/Window_function
                #waltmaxind = np.argmax( np.abs( walt))
                w = w/ w[wmaxind]
                #walt = walt/walt[waltmaxind]
                #sortind = np.argwhere( self.faxis > 0.0).flatten()
                print( 'shape of w is:\t{}'.format( np.shape( w)))
                #print( 'shape of walt is:\t{}'.format( np.shape( walt)))
                print( 'np.shape( self.faxis[self.posind]) is:\t{}'.format( np.shape( self.faxis[self.posind])))
                plt.semilogx( self.faxis[self.posind], w, 'g')
                #plt.semilogx( self.faxis[self.posind], walt, 'r')
                #plt.plot( self.faxis[sortind], poswin[sortind])
                plt.grid(True)
                plt.show()
                #sys.exit()

        
        """
        take care if PeriodAxis is used
        """
        #if( not self.PeriodAxis):
        #    lwslope = w[wmaxind:]
        #else:
        #    lwslope = w[0:wmaxind]
        
        
        if( not self.PeriodAxis):
            selind = np.arange( 0, wmaxind)
            #selind = np.arange( wmaxind, len( w) - rectlen)
            
            lwslope = w[selind]
            if( self.debug):
                print( 'self.PeriodAxis; indices of w are {}'.format( selind))
                print( 'np.shape( lwslope) is:\t{}'.format( np.shape( lwslope)))
        else:
            #selind = np.arange( wmaxind, len( w) - rectlen)
            #selind = np.arange( 0, wmaxind - rectlen + 1)
            selind = np.arange( wmaxind, len( w))
            lwslope = w[selind]#[::-1]
            if( self.debug):
                print( 'indices of lwslope are {}'.format( selind))
                print( 'np.shape( lwslope) is:\t{}'.format( np.shape( lwslope)))
        negselind = np.copy( selind)
        if( self.debug):
            #w = w/ w[wmaxind]
            #plt.loglog( self.faxis[self.posind], w, 'b', alpha = 0.6, label = 'lowerslope_window')
            #sortind = np.argwhere( self.faxis[selind]).flatten()
            
            print( 'shape of lwslope is:\t{}'.format( np.shape( lwslope)))
            print( 'shape of self.faxis[self.posind][negselind] is:\t{}'.format( np.shape( self.faxis[self.posind][negselind])))
            print( 'self.faxis[self.posind][self.ubind] is:\t{}'.format( self.faxis[self.posind][self.ubind]))
            print( 'shape of self.faxis[self.posind][self.lbind:] is:\t{}'.format( np.shape( self.faxis[self.posind][self.lbind:])))
            
            #sortind = posselind#np.argwhere( self.faxis[posselind]).flatten()

            print( 'shape of upslope is:\t{}'.format( np.shape( upslope)))
            print( 'shape of self.faxis[self.posind][posselind] is:\t{}'.format( np.shape( self.faxis[self.posind][posselind])))
            print( 'self.faxis[self.posind][self.lbind] is:\t{}'.format( self.faxis[self.posind][self.lbind]))
            print( 'shape of self.faxis[self.posind][:self.lbind] is:\t{}'.format( np.shape( self.faxis[self.posind][:self.lbind])))
            if( self.debug):
                plt.loglog( self.faxis[self.posind][self.lbind:], lwslope, '+b', alpha = 0.4, label = 'lwslope')
                if( not self.PeriodAxis):
                    plt.loglog( self.faxis[self.posind][self.lbind:self.ubind], rect, '+g', alpha = 0.4, label = 'rect')
                else:
                    plt.loglog( self.faxis[self.posind][self.ubind:self.lbind], rect, '+g', alpha = 0.4, label = 'rect')
                plt.loglog( self.faxis[self.posind][:self.ubind], upslope, 'xr', alpha = 0.2, label = 'upslope')
                plt.grid(True)
                plt.legend( loc = 'best')
                plt.show()
            #print( 'shape of upslope is:\t{}'.format( np.shape( upslope)))
            #print( 'self.faxis[self.posind][selind] is:\t{}'.format( self.faxis[self.posind][selind]))
            #print( 'shape of self.faxis[self.posind][selind] is:\t{}'.format( np.shape( self.faxis[self.posind][selind])))
            #plt.loglog( self.faxis[self.posind][selind], upslope)
            #plt.plot( self.faxis, w)
            #plt.grid(True)
            #plt.show()
            #plt.plot( self.faxis[wmaxind:][sortind], upslope)
            #plt.grid(True)
            #plt.legend( loc = 'best')
            #plt.show()
            
            #sys.exit()
        
        if( self.debug):
            print( 'shape of lwslope is:\t{}'.format( np.shape( lwslope)))
            print( 'shape of upslope is:\t{}'.format( np.shape( upslope)))
        
        #poswin = np.hstack( ( lwslope, rect, upslope))
        if( self.ubind != self.lbind):
            if( not self.PeriodAxis):
                poswin = np.hstack( ( lwslope, rect, upslope))
            else:
                poswin = np.hstack( ( upslope, rect, lwslope))

        else: # taking care for cases when the self.ubind == self.lbind and therefore there is no rectangle part
            if( not self.PeriodAxis):
                poswin = np.hstack( ( lwslope[:-1:], upslope))
            else:
                poswin = np.hstack( ( upslope, lwslope[:-1:]))
                if( self.debug):
                    print( 'len( poswin)*2', len( poswin)*2 - 1)
                    print( 'self.len', self.len)
                if( len( poswin)*2 - 1 != self.len):
                    while( len( np.hstack( ( upslope, lwslope[:-1:])))*2 - 1 != self.len):
                        if( self.debug):
                            for el, name in zip( [ len( lwslope), len( upslope), len( rect)], [ 'len( lwslope)', 'len( upslope)', 'len( rect)']):
                                print( 'len {} is: {}'.format( name, el))
                            print( 'len of self.len: {}, len( np.hstack( ( upslope, lwslope[:-1:])))*2 - 1: {} '.format( self.len, len( np.hstack( ( upslope, lwslope[:-1:])))*2 - 1))
                            #sys.exit()
                            if( len( lwslope) == 0):
                                sys.exit()
                        lwslope = lwslope[:-1:]
                        poswin = np.hstack( ( upslope, lwslope))
                if( self.debug):
                    print( 'upslope', upslope)
                    print( 'lwslope', lwslope)
                    #print( 'lwslope[:-1:]', lwslope[:-1:])
        if( self.debug):
            print( 'shape of poswin is:\t{}'.format( np.shape( poswin)))
            
                
        
        #poswin = poswin[posind]
        
        
        if( self.debug):
            #sortind = np.argwhere( self.faxis > 0.0).flatten()
            print( 'shape of poswin is:\t{}'.format( np.shape( poswin)))
            print( 'shape of self.faxis[self.posind] is:\t{}'.format( np.shape( self.faxis[self.posind])))
            plt.semilogx( self.faxis[self.posind], poswin)
            #plt.plot( self.faxis[sortind], poswin[sortind])
            plt.grid(True)
            plt.show()
            sys.exit()
        

        
        
        
        ######
        # neg frequencies
        ######
        if( len( self.posind) == len( self.negind)):
            negwin = poswin[::-1]
        else:
            negwin = poswin[::-1]
            if( len( self.posind) + len( self.negind) != len( poswin) + len( negwin)):
                negwin = np.hstack( ( negwin, negwin[-1]))
        
        if( ( len( poswin) + len( negwin) != np.max( np.shape( self.fdata))) | ( len( poswin) + len( negwin) != self.len)):
            print( 'np.mod( self.len, 2) == 0', np.mod( self.len, 2) == 0)
            print( 'len( poswin)', len( poswin), 'len( negwin)', len( negwin))
            print( 'len( poswin) + len( negwin)', len( poswin) + len( negwin))
            print( 'len( self.posind)', len( self.posind), 'len( self.negind)', len( self.negind))
            print( 'len( self.posind) + len( self.negind)', len( self.posind) + len( self.negind))
            print( 'np.shape( self.fdata)', np.shape( self.fdata))
            print( 'self.len', self.len)
            #if( np.mod( self.len, 2) == 0):
            #    if( ( len( poswin) + len( negwin) != np.max( np.shape( self.fdata))) | ( len( poswin) + len( negwin) != self.len)):
            #        sys.exit()
            #else:
            #    if( ( len( poswin) + len( negwin) != np.max( np.shape( self.fdata))) | ( len( poswin) + len( negwin) != self.len)):
            sys.exit()
            #negwin = np.hstack( ( negwin, negwin[-1]))
        
        if( self.debug):
            w = w/ w[wmaxind]
            sortind = np.argwhere( self.faxis < 0.0).flatten()
            print( 'shape of poswin is:\t{}'.format( np.shape( poswin)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            #plt.semilogx( self.faxis[self.posind], self.fdataAng[self.posind, :])
            plt.plot( self.faxis[sortind], negwin)
            plt.grid(True)
            plt.show()
            sys.exit()
        
        #w = poswin + negwin
        if( self.debug):
            print( 'self.len', self.len)
        if( False):
            if( np.mod( self.len, 2) == 0):
                w = np.hstack( ( negwin, poswin))
            else:
                w = np.hstack( ( negwin, poswin[1::]))
        w = np.hstack( ( negwin, poswin))
        
        if( self.debug):
            print( 'len( w)', len( w))
            print( 'np.shape( self.fdata)', np.shape( self.fdata))
            if( len( w) != np.max( np.shape( self.fdata))):
                sys.exit()
        w = w/ np.nanmax( w)
        if( self.kind == 'block'):
            w = np.ones( np.shape( w)) - w
        elif( self.kind == 'pass'):
            pass
        else:
            print( '\n\n\nWrong input for "kind"-option...stopping')
            return
        #sys.exit()
        #if( False):
        #    w = w + np.hstack( ( np.zeros( ( self.ctrind - 1, ) ), 1.0, np.zeros( ( N - self.ctrind,))))
        self.filter = w
        
        if( self.debug):
            print( 'shape of self.filter is:\t{}'.format( np.shape( self.filter)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            sortind = np.argsort( self.faxis).flatten()
            sortind = np.argwhere( self.faxis).flatten()
            #plt.semilogx( self.faxis[self.posind], self.fdataAng[self.posind, :])
            #plt.semilogx( self.faxis - np.min( self.faxis), np.abs( self.filter))
            plt.plot( self.faxis[sortind], np.abs( self.filter)[sortind])
            plt.grid(True)
            plt.show()
            sys.exit()
        
        self.__myFFTproc__()
        if( self.debug):
            print( 'shape of self.filter is: {}'.format( np.shape( self.filter)))
            print( 'shape of self.fdata is: {}'.format( np.shape( self.fdata)))
        #scale = self.AmplitudeScale#np.sqrt( self.fmin* self.dt**2.0)
        absfdata = np.abs( self.fdata)# will be done in __myInvFFTProc__ /self.AmplitudeScale
        angfdata = np.angle( self.fdata)
        if( self.debug):
            print( 'shape of self.filter is: {}'.format( np.shape( self.filter)))
            print( 'shape of self.fdata is: {}'.format( np.shape( self.fdata)))
            print( 'shape of absfdata is: {}'.format( np.shape( absfdata)))
            print( 'shape of angfdata is: {}'.format( np.shape( angfdata)))
        if( self.dim > 1):
            try:
                self.filtfdata = ( absfdata * np.atleast_2d( self.filter).T) * np.exp( 1j* angfdata)
            except:
                self.filtfdata = ( absfdata * self.filter) * np.exp( 1j* angfdata)
        else:
            self.filtfdata = absfdata * self.filter * np.exp( 1j* angfdata)
        
        #self.fdataMag = fdata.T
        if( self.debug):
            print( 'shape of self.filtfdata is:\t{}'.format( np.shape( self.filtfdata)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            #plt.semilogx( self.faxis[self.posind], self.fdataAng[self.posind, :])
            #plt.plot( self.faxis, np.abs( self.filtfdata))
            filtmax = np.max( np.abs( absfdata[self.posind,:]))/ np.max( np.abs( self.filter[self.posind]))
            plt.loglog( self.faxis[self.posind], np.abs( absfdata[self.posind,:]), 'g', alpha = 0.2)
            plt.loglog( self.faxis[self.posind], np.abs( self.filtfdata[self.posind,:]), 'r', alpha = 0.2)
            plt.loglog( self.faxis[self.posind], np.abs( self.filter[self.posind])* filtmax, 'b', alpha = 0.1)
            plt.grid(True)
            plt.show()
        if( self.debug):
            print( 'shape of self.filtfdata is:\t{}'.format( np.shape( self.filtfdata)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            #plt.semilogx( self.faxis[self.posind], self.fdataAng[self.posind, :])
            #plt.plot( self.faxis, np.abs( self.filtfdata))
            filtmax = np.max( np.abs( absfdata[self.negind,:]))/ np.max( np.abs( self.filter[self.negind]))
            plt.loglog( np.abs( self.faxis)[self.negind], np.abs( absfdata[self.negind,:]), 'g', alpha = 0.2)
            plt.loglog( np.abs( self.faxis)[self.negind], np.abs( self.filtfdata[self.negind,:]), 'r', alpha = 0.2)
            plt.loglog( np.abs( self.faxis)[self.negind], np.abs( self.filter[self.negind])* filtmax, 'b', alpha = 0.1)
            plt.grid(True)
            plt.show()
        #self.filtdata = np.real( fft.ifft( fft.ifftshift( self.filtfdata, axes = self.axis), axis = self.axis))#[self.orgStartInd:self.orgEndInd, :]
        self.__myInvFFTProc__()
        #self.filtdata = np.real( fft.ifft( self.filtfdata))[self.orgStartInd:self.orgEndInd, :]
        if( self.debug):
            filtmax = np.max( np.std( self.data.T))/ np.max( np.std( self.filtdata[self.orgStartInd:self.orgEndInd]))
            try:
                plt.plot( self.augt[self.orgStartInd:self.orgEndInd], self.data, 'g', alpha = 0.2)
            except:
                plt.plot( self.augt[self.orgStartInd:self.orgEndInd], self.data.T, 'g', alpha = 0.2)
            plt.plot( self.augt[self.orgStartInd:self.orgEndInd], self.filtdata[self.orgStartInd:self.orgEndInd]* filtmax, 'r', alpha = 0.2)
            plt.grid(True)
            plt.show()
            sys.exit()
        return self.augt[self.orgStartInd:self.orgEndInd], self.filtdata
    
    
    
    def GetNoiseLevel( self, **kwargs):
        
        self.perc = 0.05
        try:
            for item, value in kwargs.items():
                if( item == 'data'):
                    """
                    
                    input alternative data
                    """
                    if( self.debug):
                        print( 'alternative value for data is:\n{}'.format( value))
                        print( 'shape of alternative value for data is:\n{}'.format( np.shape( value)))
                    self.fdataMag = value
            for item, value in kwargs.items():
                if( item == 'perc'):
                    """
                    
                    input alternative percentage
                    """
                    if( self.debug):
                        print( 'alternative value for data is:\n{}'.format( value))
                        print( 'shape of alternative value for data is:\n{}'.format( np.shape( value)))
                    self.perc = value
        except:
            self.__MySlepianApproachMag__()
        d = self.fdataMag
        if( self.debug):
            print( 'self.fdataMag is:\n{}'.format( d))
            #sys.exit()
        if( d.ndim > 1):
            nN, nM = np.shape( d)
            N = np.nanmax( [nN, nM])
        else:
            N = len( d)
        ######
        # idea
        ######
        lgperc = np.logspace(np.log10( 1.0/float( N)), 1.0, N)
        lgperc = lgperc[::-1]
        maxleng = np.argmin( np.abs( self.perc - lgperc))
        if( True):#self.debug):
            print( 'with self.perc: {} one gets maxleng: {}'.format( self.perc, maxleng))
        ######
        #maxleng = int( self.perc * float( N))
        if( maxleng < 100):
            raise self.MyException('data array to short with length {}'.format( maxleng))
        r = []
        #inc = 10
        inc = int( float( maxleng)/ 100.0) # get the hundret best values
        if( np.ndim( d) == 1):
            check = np.abs( d[::-1])[:maxleng]
            if( False):
                plt.hist( check, alpha = 0.3)
                plt.show()
            for k in np.arange( 0, maxleng, inc):
                sl = check[k: k + inc]
                r.append( np.max( sl)) #taking only the biggest sample per slice
        elif( np.ndim( d) == 2):
            if( np.argmax( np.shape( d)) == 0):
                check = ( np.abs( d[::-1, :])[:maxleng, :]).reshape( -1, 1) # working but not good for hourly plots
            else:
                check = ( np.abs( d[:, ::-1])[:, :maxleng]).reshape( -1, 1) # working but not good for hourly plots
            for k in np.arange( 0, maxleng, inc):
                sl = check[k: k + inc]
                r.append( np.max( sl)) #taking only the biggest sample per slice
        #self.noise = np.sqrt( np.pi)* np.sqrt( np.mean( np.power( r, 2.0)))
        self.noise = np.sqrt( np.pi)* np.sqrt( np.median( np.power( r, 2.0)))
        
        if( False):
            newnoise = np.sqrt( np.pi)* np.sqrt( mode( np.power( r, 2.0)))
            print( 'self.noise is:\t{}'.format( self.noise), '\tnewnoise is:\t{}'.format( newnoise))
            plt.loglog( np.logspace(np.log10( 1.0/float( N)), 1.0, N), d, alpha = 0.2)
            ones = np.ones( ( N, ))
            plt.loglog( np.logspace(np.log10( 1.0/float( N)), 1.0, N), ones* self.noise, 'g', alpha = 0.2)
            plt.loglog( np.logspace(np.log10( 1.0/float( N)), 1.0, N), ones* newnoise, 'b', alpha = 0.2)
            plt.show()
        if( self.debug):
            print( 'self.noise is:\n{}'.format( self.noise))
            #sys.exit()
        return self.noise
    
    
    
    def __Ret2OldLen__( self):
        
        #scale = self.AmplitudeScale#np.sqrt( self.fmin* self.dt**2.0)
        #print( '\nshape of self.faxis\t{}'.format( np.shape( self.faxis)))
        #print( '\nshape of self.fdata\t{}'.format( np.shape( self.fdata)))
        #print( '\nshape of self.fDataMag\t{}'.format( np.shape( self.fdataMag)))
        #print( '\nshape of self.fdataAng\t{}'.format( np.shape( self.fdataAng)))
        #print( 'shape of self.filter', np.shape( self.filter))
        #sys.exit()
        self.len = self.shape[self.axis]
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        #self.t = np.arange( 0, float( self.len)* self.dt, self.dt)
        self.t = np.arange( 0, float( self.len))* self.dt
        if( self.debug):
            print( 'self.t is: {}'.format( self.t))
        #print( 'self.t is: {}'.format( np.shape( self.t)))
        #print( 'self.augt is: {}'.format( np.shape( self.augt)))
        #print( 'self.faxis is: {}'.format( np.shape( self.faxis)))
        #sys.exit()
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.fnyq is: {}'.format( self.fnyq))
        if( self.debug):
            print( 'self.dt is: {}'.format( self.dt))
        self.fmin = 1.0/ float( self.len)/ self.dt
        if( self.debug):
            print( 'self.fmin is: {}'.format( self.fmin))
        bakax = self.faxis
        if( self.debug):
            print( 'shape of bakax', np.shape( bakax))
            print( 'shape of self.posind before length reduction', np.shape( self.posind))
        self.len = self.baklen
        self.__faxis__()
        if( self.debug):
            print( 'shape of self.faxis', np.shape( self.faxis))
            print( 'shape of self.posind before length reduction', np.shape( self.posind))
        if( False):
            if( self.PeriodAxis):
                #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
                #self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq + self.fmin, self.fmin)
                bakax = self.faxis
                self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
            else:
                #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
                #self.faxis =  np.arange( -self.fnyq, self.fnyq + self.fmin, self.fmin)
                bakax = self.faxis
                self.faxis =  np.arange( -self.fnyq, self.fnyq, self.fmin)
        if( self.aug == True):
            if( self.axis == 0):
                for el in self.__dict__.keys():
                    #print( 'Checking length of {}'.format( el))
                    #####or el.startswith( 'faxis')
                    if( el.startswith( 'fdata') or el.startswith( 'filtf') or el.startswith( 'filte')):
                        if( self.debug):
                            print( 'el is', el)
                        if( True):
                            varname = 'self.' + el
                            if( self.debug):
                                print( 'changing shape of {} from {}'.format( varname, np.shape( self.__dict__[el])))
                                print( 'to shape of {} taken from self.faxis'.format( np.shape( self.faxis)))
                        if( self.__dict__[el].ndim > 1):
                            if( np.argmax( np.shape( self.data)) != np.argmax( np.shape( self.__dict__[el]))):
                                if( self.debug):
                                    print( 'transposing {} with shape {} to shape {}'.format( el, np.shape( self.__dict__[el]), np.shape( self.__dict__[el].T)))
                                self.__dict__[el] = self.__dict__[el].T
                                if( self.debug):
                                    print( 'transposed {} to shape {}'.format( el, np.shape( self.__dict__[el])))
                            if( self.debug):
                                print( 'bakax', 'self.__dict__[el]', 'self.axis', 'self.rplval')
                                for bl, name in zip( [bakax, self.__dict__[el], self.axis, self.rplval], ['bakax', 'self.__dict__[el]', 'self.axis', 'self.rplval']):
                                    print( 'shape of {} is {}\n\tvals: {}'.format( name, np.shape( bl), bl))
                                #if( el.startswith( 'filtf')):
                                #    print( 'el is', el)
                                #    print( 'shape of {} is {}, vals: {}'.format( el, np.shape( self.__dict__[el]), self.__dict__[el]))
                                #    plt.plot( bakax, self.__dict__[el], 'r', alpha = 0.3)
                                #    plt.show()
                            #self.__dict__[el] = ipol.interp1d( bakax, self.__dict__[el], axis = self.axis, bounds_error=False, kind=self.rplval, fill_value = 'extrapolate')(self.faxis)
                            #print( 'el is', el)
                            #print( 'shape of {} is {}, vals: {}'.format( el, np.shape( self.__dict__[el]), self.__dict__[el]))
                            #print( 'shape of {} is {}, vals: {}'.format( 'bakax', np.shape( bakax), bakax))
                            self.__dict__[el] = ipol.interp1d( bakax, self.__dict__[el].reshape( np.shape( self.__dict__[el])), axis = self.axis, bounds_error=False, kind=self.rplval, fill_value = 'extrapolate')(self.faxis)
                        else:
                            #self.__dict__[el] = self.__dict__[el]
                            self.__dict__[el] = ipol.interp1d( bakax, self.__dict__[el], bounds_error=False, kind=self.rplval, fill_value = 'extrapolate')(self.faxis)
                        #print( 'changed shape of {} to {}'.format( varname, np.shape( self.__dict__[el])))
                    if( el.startswith( 'filtd')):
                        #if( self.debug):
                        if( True):
                            varname = 'self.' + el
                            if( self.debug):
                                print( 'changing shape of {} from {}'.format( varname, np.shape( self.__dict__[el])))
                        if( self.__dict__[el].ndim > 1):
                            if( np.argmax( np.shape( self.data)) != np.argmax( np.shape( self.__dict__[el]))):
                                self.__dict__[el] = self.__dict__[el].T
                            if( self.debug):
                                print( 'shape of {} is {}, vals: {}'.format( el, np.shape( self.__dict__[el]), self.__dict__[el]))
                                print( 'shape of {} is {}, vals: {}'.format( 'self.augt', np.shape( self.augt), self.augt))
                            self.__dict__[el] = ipol.interp1d( self.augt, self.__dict__[el].reshape( np.shape( self.__dict__[el])), axis = self.axis, bounds_error=False, kind=self.rplval, fill_value = 'extrapolate')(self.t.T)
                        else:
                            #self.__dict__[el] = self.__dict__[el]
                            self.__dict__[el] = ipol.interp1d( self.augt, self.__dict__[el], bounds_error=False, kind=self.rplval, fill_value = 'extrapolate')(self.t.T)
                        #print( 'changed shape of {} to {}'.format( varname, np.shape( self.__dict__[el])))
            elif( self.axis == 1):
                
                for el in self.__dict__.keys():
                    #print( 'Checking length of {}'.format( el))
                    #####or el.startswith( 'faxis')
                    if( el.startswith( 'fdata') or el.startswith( 'filtf') or el.startswith( 'filte')):
                        if( self.debug):
                            varname = 'self.' + el
                            if( self.debug):
                                print( 'changing shape of {} from {}'.format( varname, np.shape( self.__dict__[el])))
                                print( 'np.shape( self.data)', np.shape( self.data))
                        if( self.__dict__[el].ndim > 1):
                            if( np.argmax( np.shape( self.data)) != np.argmax( np.shape( self.__dict__[el]))):
                                self.__dict__[el] = self.__dict__[el].T
                            self.__dict__[el] = ipol.interp1d( bakax, self.__dict__[el], axis = self.axis, bounds_error=False, kind=self.rplval, fill_value = 'extrapolate')(self.faxis)
                        else:
                            #self.__dict__[el] = self.__dict__[el]
                            #print( 'changing shape of {} from {}'.format( varname, np.shape( self.__dict__[el])))
                            self.__dict__[el] = ipol.interp1d( bakax, self.__dict__[el], bounds_error=False, kind=self.rplval, fill_value = 'extrapolate')(self.faxis)
                        #print( 'changed shape of {} to {}'.format( varname, np.shape( self.__dict__[el])))
                    if( el.startswith( 'filtd')):
                        if( self.debug):
                            varname = 'self.' + el
                            if( self.debug):
                                print( 'changing shape of {} from {}'.format( varname, np.shape( self.__dict__[el])))
                        if( self.__dict__[el].ndim > 1):
                            if( np.argmax( np.shape( self.data)) != np.argmax( np.shape( self.__dict__[el]))):
                                self.__dict__[el] = self.__dict__[el].T
                            self.__dict__[el] = ipol.interp1d( self.augt, self.__dict__[el], axis = self.axis, bounds_error=False, kind=self.rplval, fill_value = 'extrapolate')(self.t.T)
                        else:
                            #self.__dict__[el] = self.__dict__[el]
                            #print( 'changing shape of {} from {}'.format( varname, np.shape( self.__dict__[el])))
                            self.__dict__[el] = ipol.interp1d( self.augt, self.__dict__[el], bounds_error=False, kind=self.rplval, fill_value = 'extrapolate')(self.t.T)
                        #print( 'changed shape of {} to {}'.format( varname, np.shape( self.__dict__[el])))
        #print( '\nshape of self.faxis\t{}'.format( np.shape( self.faxis)))
        #print( '\nshape of self.fdata\t{}'.format( np.shape( self.fdata)))
        #print( '\nshape of self.fDataMag\t{}'.format( np.shape( self.fdataMag)))
        #print( '\nshape of self.fdataAng\t{}'.format( np.shape( self.fdataAng)))
        #sys.exit()
        if( self.debug):
            for el in self.__dict__.keys():
                if( el.startswith( 'fdata') or el.startswith( 'filt')):
                    if( self.axis == 0):
                        plt.loglog( self.faxis, np.abs( self.__dict__[el]))
                    elif( self.axis == 1):
                        plt.loglog( self.faxis, np.abs( self.__dict__[el]).T)
            plt.grid(True)
            plt.show()
        if( self.debug):
            print( 'faxis len ', np.shape( self.faxis))
            print( 'faxis fdataMag shape ', np.shape( self.fdataMag))
            #plt.loglog( self.faxis, np.abs( self.fdataMag).T)
            #plt.grid(True)
            #plt.show()
            #sys.exit()
        #sys.exit()
        #self.__faxis__()
        return
    
    
    def fdata( self):
        self.__myFFTproc__()
        if( self.RemoveSubHarm):
            dummy = []
            for el in self.fdata:
                el, Maxind, Maxvecs, Maxvals, MaxVt, MaxApproxSerLen = self.PCARotate( el)
                dummy.append( el)
            dummy = np.array( dummy, dtype = complex).T
            self.fdata = dummy
            del dummy
        self.__Ret2OldLen__()
        return self.faxis, self.fdata
    
    
    
    def fdataMag( self):
        self.__MySlepianApproachMag__()
        if( self.RemoveSubHarm):
            self.fdataMag = self.PCARotate( self.fdataMag)
        if( self.debug):
            
            RefSpectra = fft.fftshift( fft.fft( self.augdata, axis = self.axis), axes = self.axis)
            for k, ( fd, cfd) in enumerate( zip( self.fdataMag, RefSpectra)):                
                plt.loglog( self.faxis[self.posind], np.abs( fd[self.posind]), alpha = 0.3, label = 'fdataMag {}'.format( k))
                plt.loglog( self.faxis[self.posind], np.abs( cfd[self.posind]), alpha = 0.3, label = 'RefSpectra {}'.format( k))
                plt.legend( loc = 'best')
                plt.grid( which = 'both')
                plt.show()
        self.__Ret2OldLen__()
        
        if( True):#self.debug):
            
            RefSpectra = fft.fftshift( fft.fft( self.data, axis = self.axis), axes = self.axis)
            for k, ( fd, cfd) in enumerate( zip( self.fdataMag, RefSpectra)):                
                plt.loglog( self.faxis[self.posind], np.abs( fd[self.posind]), alpha = 0.3, label = 'fdataMag {}'.format( k))
                plt.loglog( self.faxis[self.posind], np.abs( cfd[self.posind]), alpha = 0.3, label = 'RefSpectra {}'.format( k))
                plt.legend( loc = 'best')
                plt.grid( which = 'both')
                plt.show()
        return self.faxis, self.fdataMag
    
    
    
    def fdataAng( self):
        self.__MySlepianApproachAng__()
        self.__Ret2OldLen__()
        return self.faxis, self.fdataAng
    
    
    
    def frqfilter( self):
        if( self.type == 'exp'):
            self.__expfilt__()
        elif( self.type == 'lbub'):
            self.__lbubfilt__()
        self.__Ret2OldLen__()
        return self.faxis, self.filtfdata, self.filter, self.filtdata
    
    
    
    def AnaSig( self, **kwargs):
        #avgaseries = np.atleast_2d( self.data.mean( axis = self.axis)).T
        #print( 'shape of avgaseries is:\n{}'.format( np.shape( avgaseries)))
        #dump = (self.data - avgaseries).T
        #dump = self.data
        #print( 'shape of dump is: \n{}'.format( np.shape( dump)))
        avgdata = self.data.mean( axis = self.axis)[np.newaxis, :].T
        self.data = self.data - avgdata
        if( self.aug):
            atseries, aaseries, orgstart, orgend = self.FIELDAUGMENT( self.t, self.data)
            self.FIELDAUGMENT( self.t, self.data)
        else:
            """ if there is no field augmentation applied"""
            self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.t, self.data, 0, self.shape[self.axis] - 1
        #    #scale = float(np.max( np.shape( atseries)))/ float(np.max( np.shape( dump)))
        #print( 'shape of aaseries is: \n{}'.format( np.shape( aaseries)))
        print( 'Deriving analytical signals...')
        #self.__faxis__()
        #self.__myFFTproc__()
        #plt.semilogx( self.faxis[self.posind], np.angle( self.fdata.T)[self.posind])
        #plt.semilogx( -self.faxis[self.negind], np.angle( self.fdata.T)[self.negind])
        #plt.show()
        #self.filtfdata = self.fdata
#        if( self.axis == 0):
#            self.filtfdata[ np.argwhere( self.faxis < 0).flatten(),:] = self.fdata[ np.argwhere( self.faxis < 0).flatten(),:] + self.fdata[ np.argwhere( self.faxis < 0).flatten(),:]*1j # applying 90 degree phase shift to negative frequency part of signal
#        elif( self.axis == 1):
#            self.filtfdata[ :, np.argwhere( self.faxis < 0).flatten()] = self.fdata[ :, np.argwhere( self.faxis < 0).flatten()] + self.fdata[ :, np.argwhere( self.faxis < 0).flatten()]*1j # applying 90 degree phase shift to negative frequency part of signal
        #if( self.axis == 0):
        #self.filtfdata = ( self.fdata + 1j* np.sign( self.faxis)*self.fdata) # applying 90 degree phase shift to negative frequency part of signal
        #self.filtfdata = self.fdata + 1j* np.sign( self.faxis)*self.fdata # applying 90 degree phase shift to negative frequency part of signal
        #plt.semilogx( self.faxis[self.posind], np.angle( self.filtfdata.T)[self.posind])
        #plt.semilogx( -self.faxis[self.negind], np.angle( self.filtfdata.T)[self.negind])
        #plt.show()
        #sys.exit()
        #elif( self.axis == 1):
        #    self.filtfdata[ :, np.argwhere( self.faxis < 0).flatten()] = self.fdata[ :, np.argwhere( self.faxis < 0).flatten()] + self.fdata[ :, np.argwhere( self.faxis < 0).flatten()]*1j # applying 90 degree phase shift to negative frequency part of signal
        
        #plt.plot( self.filtdata.T)
        #plt.show()
        #self.__myInvFFTProc__()
        
        if( True):
            temp = sg.hilbert( self.augdata, axis = self.axis)#/scale#
            print( '...done')
            #print( 'shape of temp after hilbert transform and before cut orginal samples is :\n{}'.format( np.shape( temp)))
        if( self.aug):
            temp = temp[orgstart:orgend,:]#.T + avgaseries).T
            #self.__Ret2OldLen__()
        else:
            #temp = ( temp.T + avgaseries).T
            pass
        #temp = temp + avgaseries
        #print( 'shape of temp after hilbert transform and cut orginal samples is :\n{}'.format( np.shape( temp)))
        #hilbert3 = lambda x: sg.hilbert( x, N = fft.next_fast_len( max( np.shape(x))), axis = np.argmax( np.shape(x)))[: max( np.shape(x))]
        #temp = hilbert3( aseries)
        try:
            temp = temp + avgdata
        except:
            temp = temp + avgdata.T
        if( np.shape( self.data) != np.shape( temp)):
            temp = temp.T
        self.Anadata = temp
        #plt.plot( self.t, self.Anadata.T, 'r')
        #plt.plot( self.t, ( self.data + avgdata).T, 'g', alpha = 0.2)
        #plt.show()
        #sys.exit()
        return np.abs( temp), np.angle( temp)


    def DSpike( self):
        avgdata = self.data.mean( axis = self.axis)[np.newaxis, :].T
        self.data = self.data - avgdata
        self.DeSpikedData, self.newchgind = self.DeSpike()
        
        return self.DeSpikedData, self.newchgind
    
    """
    THE NKEND
    """