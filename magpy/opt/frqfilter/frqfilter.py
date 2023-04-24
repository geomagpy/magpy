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
import scipy.fftpack as fft
import scipy.interpolate as ipol
from itertools import chain
import time
from statistics import mode

debug = False # Print every single output

import matplotlib.pyplot as plt
print( 'pyplot imported as plt for debugging purposes')

class FrqFilter:
    """
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
                                    despike = True, despikeperc = 0.01)
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
            Define up to which percentage a timeseries will be despiked.
            Values between 0.0 and 1.0 are allowed.
            0.0 means no value will be despiked...equivalent with
            despike = False
            1.0 all found spikes according which are bigger than defined 
            "lvl" will be replaced by method "rplval".
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
                            despikeperc = 0.01)
        
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
            ###########
            # OLD
            ###########
            if( False):
                if( self.PeriodAxis):
                    #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
                    self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
                    if( self.debug):
                        print( 'self.faxis is: {}'.format( self.faxis))
                    self.posind = np.argwhere( self.faxis >= 1.0/ self.fnyq).flatten()#[0]
                    self.negind = np.argwhere( self.faxis <= -1.0/ self.fnyq).flatten()#[0]
                    if( self.debug):
                        print( 'self.faxis, self.posind, self.negind are: {}, {}, {}'.format( self.faxis, self.posind, self.negind))
                else:
                    #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
                    self.faxis =  np.arange( -self.fnyq, self.fnyq, self.fmin)
                    if( self.debug):
                        print( 'self.faxis is: {}'.format( self.faxis))
                    self.posind = np.argwhere( self.faxis >= self.fmin).flatten()#[0]
                    self.negind = np.argwhere( self.faxis <= -self.fmin).flatten()#[0]
                    if( self.debug):
                        print( 'self.faxis, self.posind, self.negind are: {}, {}, {}'.format( self.faxis, self.posind, self.negind))
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
    
    
    
    def DeSpike( self, **kwargs):
        print('\nCalling despiking data...')
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
        #except:
        #    pass
        #try:
        #    for item, value in kwargs.items():
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
        if( self.debug):
            print('shape of self.data is: {}x{}'.format( N, M))
        
        Data = np.diff( np.diff( self.data, axis = self.axis), axis = self.axis)
        #print( 'np.shape( Data)', np.shape( Data))
        if( self.axis == 0):
            #Data = self.data.T
            if( debug):
                print('shape of Data after np.diff is: {}'.format( np.shape( Data)))
            if( debug):
                print('self.axis == 0 shape of Data after np.diff is: {}'.format( np.shape( Data)))
            Data = np.hstack( ( Data.T, Data[-1,:])).T
            Data = np.hstack( ( Data[0,:], Data.T)).T
            chkavgs = np.nanmedian( Data, axis = self.axis)
            Data = Data.T
            chgData = np.copy( self.data.T)
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
            raise MyException( 'Axis for despiking is chosen to be {} but this axis is not available'.format( self.axis))
        if( self.debug):
            print('shape of Data after stacking is: {}'.format( np.shape( Data)))
            print('shape of ckgavgs after stacking is: {}'.format( np.shape( chkavgs)))
            print('shape of ckgData is: {}'.format( np.shape( chgData)))
        time = self.t
        if( self.debug):
            try:
                try:
                    for el in Data:
                        plt.plot( time, el)
                except:
                    for el in Data.T:
                        plt.plot( time, el)
                plt.show()
            except:
                print( 'shape of time is:\t{}'.format( np.shape( time)))
                print( 'shape of Data is:\t{}'.format( np.shape( Data)))
        if( self.debug):
            print( 'Data is:\t{}\ntime is:\t{}\nrplval is:\t{}\nlvl is:\t{}\n'.format( Data, time, rplval, lvl))
            print( 'shape of Data is:\t{}\nshape of time is:\t{}\n'.format( np.shape( Data), np.shape( time)))
            #sys.exit()
        
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
        changeind = []
        mygoodind = []
        for d, av, lv in zip( Data, chkavgs.T, lvl.T):
            #changeind.append( np.where( np.logical_or( np.logical_or( np.abs( d - av) > lv, np.isnan( d)), np.isinf( d))))
            
            if( self.debug):
                print( 'd is:\t{}\nav is:\t{}\nlv is:\t{}\n'.format( d, av, lv))
                print( 'np.shape( d)', np.shape( d))
                
            b = np.isnan( d)
            c = np.isinf( d)
            hind = np.argwhere( b | c).flatten()#[0]#.flatten()
            d[hind] = av
            a = np.abs( d - av) > lv
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
            if( percchk <= self.despikeperc):
                changeind.append( e)
                mygoodind.append( f)
            else:
                print( 'Exceeding despiking limit of {} percent'.format( self.despikeperc * 100.0))
                changeind.append( [])
                mygoodind.append( np.argwhere( np.ones( ( self.len, ), dtype = bool)).flatten())
            #print( 'changeind is: {}'.format( changeind[-1]))
        #changeind = np.where( np.abs( Data - np.atleast_2d( avgs).T) > np.atleast_2d( lvl).T)
        if( self.debug):
            sys.stdin.readline()
        newchgind = np.array( changeind)
        mygoodind = np.array( mygoodind)
        if( self.debug):
            #print( 'np.shape( newchgind[k]) is: {}'.format( np.shape( newchgind[k])))
            print( 'np.shape( mygoodind) is: {}'.format( np.shape( mygoodind)))
            print( 'np.shape( newchgind) is: {}'.format( np.shape( newchgind)))
        #########
        #newchgind = []
        #for li in changeind:
        #    for el in li:
        #        if( not el in newchgind):
        #            newchgind.append(el[0])
        #newchgind = np.unique( newchgind)
        checklenlist = list( chain( *newchgind))
        sumbadind = []
        if( len( checklenlist) >= 1):
            if( self.debug):
                print( 'checklenlist is:\t{}\n'.format( checklenlist))
                print( 'len of checklenlist is:\t{}\n'.format( len( checklenlist)))
                #sys.exit()
                if( self.debug):
                    for k, el in enumerate( newchgind):
                        print( '\nData[{}]\t=\t{}'.format( k, chgData[k, el]))
                    sys.stdin.readline()
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
                                goodind = ind
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
                        for k, (d, ind, badind) in enumerate( zip( chgData, mygoodind, newchgind)):
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
                                raise self.MyException( "[{}]-th column has some nan's".format( k, d))
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
            for el in chgData:
                plt.plot( self.t, el)
            plt.show()
        #self.data = Data
        if( self.axis == 0):
            self.despdata = chgData
            #chkData = chkData.T
        elif( self.axis == 1):
            self.despdata = chgData
        else:
            raise MyException( 'Error during transposition to original dimensions with axis {}'.format( self.axis))
        
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
        
        return self.despdata, newchgind
    
    
    
    
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
        from numpy import float, int, arange, max, nanmean, vstack, mod#, polyfit, polyval
        from numpy import zeros
        from scipy.signal import parzen#, exponential
        from scipy import polyfit, polyval
        
        
        
        if( len( args) == 2):
            
            aseries = args[1]
            tseries = args[0]
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
            #!!! FIELD AUGMENTATION NOT WORKING PROPERLY... CHANGE atseries ... ,maybe!!!
            U, V = shape(three_aseries) # dimensions of three_aseries
            minDim, maxDim = [ argmin( [U, V]), argmax( [U, V])]
            three_length = [ U, V][self.axis]
            three_columns = [ U, V][not self.axis]
            
            atseries = tseries[0] + arange( tseries[0] - tseries[-1] , 2.0*( tseries[-1] - tseries[0]) + dt, dt)
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
                k = 2
                tas = (three_aseries - np.atleast_2d( nanmean( three_aseries, axis = self.axis)).T)
                aas = (aseries - np.atleast_2d( nanmean( aseries, axis = self.axis)).T)
                print( 'np.shape( tas)', np.shape( tas))
                print( 'np.shape( atseries)', np.shape( atseries))
                print( 'np.shape( tseries)', np.shape( tseries))
                print( 'np.shape( aseries)', np.shape( aseries))
                if( self.axis == 0):
                    plt.plot( atseries, tas[:,k], 'r')
                    plt.plot( tseries, aas[:,k], 'g')
                elif( self.axis == 1):
                    plt.plot( atseries, tas[k,:], 'r')
                    plt.plot( tseries, aas[k,:], 'g')
                plt.show()
                sys.exit()
            
            
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
                k = 2
                k = 2
                #plt.plot( atseries, (aaseries.T - nanmean( aaseries[:, orgStartInd: orgEndInd], axis = 1))[:,k], 'r')
                if( self.axis == 1):
                    plt.plot( atseries, aaseries.T[:,k], 'r')
                    #plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                    plt.plot( atseries[trendind[0]:trendind[-1] + 1], trendfct[k,:], 'b')
                    plt.plot( atseries[trendind[0]:trendind[-1] + 1], taper[maxind + 1::]*max( trendfct[k,:]), 'g')
                elif( self.axis == 0):
                    plt.plot( atseries, aaseries.T[k,:], 'r')
                    #plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                    plt.plot( atseries[trendind[0]:trendind[-1] + 1], trendfct[:,k], 'b')
                    #plt.plot( atseries[trendind[0]:trendind[-1] + 1], taper[maxind + 1::]*max( trendfct[:,k]), 'g')                    
                plt.show()
                sys.exit()
            
            
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
            self.data, changeind = self.DeSpike()
        if( self.aug):
            """ if there is some field augmentation applied"""
            #self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.FIELDAUGMENT( self.t, self.data)
            self.avg = np.nanmean( self.data, axis = self.axis)
            if( self.axis == 0):
                self.FIELDAUGMENT( self.t, self.data - self.avg)
            if( self.axis == 1):
                self.FIELDAUGMENT( self.t, self.data.T - self.avg)
                self.augdata = self.augdata.T
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
            sys.exit()
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
            self.taper = sg.windows.hanning( self.len)
        
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
    
    
    
    def __MySlepianApproachMag__( self):
        
        from scipy.signal.windows import dpss
        
        #fmin = 1.0/ np.max( np.shape( self.data))/ self.dt
        
        if( self.despike):
            print( 'Despiking data...')
            self.data, changeind = self.DeSpike()
        
        if( self.debug):
            print( 'shape of self.t is:\t{}'.format( np.shape( self.t)))
        
        if( self.aug):
            """ if there is some field augmentation applied"""
            #self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.FIELDAUGMENT( self.t, self.data)
            self.avg = np.atleast_2d( np.nanmean( self.data, axis = self.axis))
            if( self.debug):
                print( 'self.axis', self.axis)
                print( 'np.shape( self.avg)', np.shape( self.avg))
                print( 'np.shape( self.data)', np.shape( self.data))
                #sys.exit()
            if( self.axis == 0):
                self.FIELDAUGMENT( self.t, self.data - self.avg)
            if( self.axis == 1):
                self.FIELDAUGMENT( self.t, self.data - self.avg.T)
                #self.augdata = self.augdata.T
            
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
                self.posind = np.argwhere( self.faxis >= 1.0/ self.fnyq).flatten()
                self.negind = np.argwhere( self.faxis <= -1.0/ self.fnyq).flatten()
            else:
                #self.faxis = np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
                self.faxis =  np.arange( -self.fnyq, self.fnyq, self.fmin)
                self.posind = np.argwhere( self.faxis >= self.fmin).flatten()
                self.negind = np.argwhere( self.faxis <= self.fmin).flatten()
        
        
        N, M = np.shape( dump)
        if( self.debug):
            print( 'Length for dpss: {}'.format( [N, M][self.axis]))
            print( 'N, M', N, M)
        #slep = dpss( [N, M][self.axis], 5, self.NumOfSlep, norm = 2)
        BW = 2.0*self.fmin
        twoNW = int( self.len* self.dt* BW) # from scipy doc(https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.windows.dpss.html): Standardized half bandwidth corresponding to 2*NW = BW/f0 = BW*N*dt where dt is taken as 1
        if( self.debug):
            print( 'twoNW', twoNW)
        #######
        # Trying to dynamically derive optimal amount of slepian windows according to "When Slepian Meets Fiedler: Putting a Focus on the Graph Spectrum - Van De Ville, Demesmaeker, Preti, 2017 IEEE Paper"
        #######
        if( self.DynNumOfSlep):
            if( self.debug):
                print( np.shape( self.data))
            #self.__myFFTproc__()
            #print( np.shape( self.fdata))
            #dummy = self.fdata[:, self.posind]#/ np.sqrt( self.fmin)
            Econcent = -10.0**10
            oldEconcent = Econcent - 1
            n = 4
            dump = self.augdata
            tempwin = sg.windows.hanning( self.len)
            reference = np.abs( fft.fftshift( fft.fft( self.augdata* tempwin, axis = self.axis)))
            magref = np.sqrt( np.nansum( reference* reference, axis = 1 - self.axis))
            reference = reference/ magref
            checkfin = np.isfinite( reference)
            checknan = ~np.isnan( reference)
            checkall = np.argwhere( np.all( checkfin & checknan, axis = 1 - self.axis)).flatten()
            if( self.axis == 0):
                reference = reference[checkall, :]
            elif( self.axis == 1):
                reference = reference[:, checkall]
            #logref = np.log10( magref)
            if( False):
                vals = []
                detrendref = sg.detrend( self.augdata, axis = self.axis)
                for dref in detrendref.T:
                    n = np.nanmax( [n, self.crossings_nonzero_all( dref)])
                n = n + 2
                print( 'Adjusting a initial {}-th order temporal polynom'.format( n - 1))
                if( False):
                    try:
                        poly = np.polyfit( np.abs(self.faxis), detrendref.T, n + 2)
                    except:
                        poly = np.polyfit( np.abs(self.faxis), detrendref, n + 2)
                    for el in poly.T:
                        #print( np.shape( el))
                        #sys.exit()
                        vals.append( np.polyval( el, np.abs(self.faxis)))
                    vals = np.array( vals)
                    if( np.shape( vals) != np.shape( reference)):
                        vals = vals.T
                    vals = ( vals.T - np.atleast_2d( np.nanmean( vals, self.axis))).T
                    #vals = np.polyval( poly, np.abs(self.faxis))
                    #checkvar = np.abs( np.sum( vals - reference, axis = 1 - self.axis))
                    #descind = np.argsort( checkvar)[::-1]
                    #descind = np.argsort( magref)[::-1]
            descind = np.argsort( np.abs( self.faxis))[::-1]
            #descind = np.argsort( magref)[::-1]
            
            myscale = np.sqrt( np.pi) # scale for iteration steps
            #reference = reference/ magref
            while( oldEconcent < Econcent and n < 50):
                oldEconcent = Econcent
                slep = dpss( [N, M][self.axis], twoNW, n, norm = 'approximate')
                
                fdata = np.zeros( ( N, M), dtype = complex)
                if( True):
                    if( self.axis == 0):
                        slep = np.atleast_2d( slep[:,-1]).T # taking only last slepian window for covariance tests
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
                            fdata[:, k] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis)), axis = 1)
                        #print( 'shape of fData in MySlepianApproach is:\n\t{}'.format( np.shape( fData)))
                        #sys.exit()
                    elif( self.axis == 1):
                        slep = np.atleast_2d( slep[-1,:]) # taking only last slepian window for covariance tests
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
                            fdata[k, :] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis)), axis = 0).T
                        #print( 'shape of fData in MySlepianApproach is:\n\t{}'.format( np.shape( fData)))
                        #sys.exit()
                    dummy = np.abs( fft.fftshift( fdata, axes = self.axis))
                else:
                    try:
                        vals = []
                        for d in detrendref.T:
                            poly = np.polynomial.polynomial.Polynomial.fit( self.augt, d, n)
                            vals.append( np.polyval( poly.coef, self.augt))
                            #print( vals)
                            #vals = *vals.np.abs(self.faxis)
                    except:
                        vals = []
                        for d in detrendref:
                            poly = np.polynomial.polynomial.Polynomial.fit( self.augt, d, n)
                            vals.append( np.polyval( poly.coef, self.augt))
                            #print( vals)
                        #vals = *vals.np.abs(self.faxis)
                    #vals = []
                    #sys.exit()
                    #for el in poly.T:
                    #    #print( np.shape( el))
                    #    #sys.exit()
                    #    vals.append( np.polyval( el, np.abs(self.faxis)))
                    vals = np.array( vals)
                    if( np.shape( vals) != np.shape( reference)):
                        vals = vals.T
                    vals = ( vals.T - np.atleast_2d( np.nanmean( vals, self.axis))).T
                    
                    dummy = np.abs( fft.fftshift( vals* tempwin, axes = self.axis))
                
                #print( dummy)
                #sys.exit()
                #reference = reference[0:np.min( np.shape( dummy)), 0:np.max( np.shape( dummy))]
                if( self.axis == 0):
                    dummy = dummy[checkall, :]
                    #eigvals = np.linalg.eigvals( np.matmul( dummy[descind[0:n], :].T, reference[descind[0:n], :])/float(n)) # maximizing according to "When Slepian Meets Fidler: Putting a Focus on the Graph Spectrum", Van de Ville, Demesmaeker, Preti 2017 IEEE
                    covmat = np.cov( dummy[descind[0:n], :].T, reference[descind[0:n],:].T)
                    autocovmat = np.cov( reference[descind[0:n], :].T, reference[descind[0:n],:].T)
                    print( 'shape of covmat is: {}'.format( np.shape( covmat)))
                    #eigvals = np.linalg.eigvals( covmat)#/ autocovmat) # maximizing according to "When Slepian Meets Fidler: Putting a Focus on the Graph Spectrum", Van de Ville, Demesmaeker, Preti 2017 IEEE
                elif( self.axis == 1):
                    dummy = dummy[:, checkall]
                    #eigvals = np.linalg.eigvals( np.matmul( dummy[:, descind[0:n]].T, reference[:, descind[0:n]])/float(n)) # maximizing according to "When Slepian Meets Fidler: Putting a Focus on the Graph Spectrum", Van de Ville, Demesmaeker, Preti 2017 IEEE
                    covmat = np.cov( dummy[:, descind[0:n]].T, reference[:, descind[0:n]].T)
                    autocovmat = np.cov( reference[:, descind[0:n]].T, reference[:, descind[0:n]].T)
                    print( 'shape of covmat is: {}'.format( np.shape( covmat)))
                    #eigvals = np.linalg.eigvals( covmat)#/ autocovmat) # maximizing according to "When Slepian Meets Fidler: Putting a Focus on the Graph Spectrum", Van de Ville, Demesmaeker, Preti 2017 IEEE
                if( ~np.all( np.isnan( covmat)) & np.all( np.isfinite( covmat))):
                    refeigvals = np.linalg.eigvals( autocovmat)#/ autocovmat) # maximizing according to "When Slepian Meets Fidler: Putting a Focus on the Graph Spectrum", Van de Ville, Demesmaeker, Preti 2017 IEEE
                    eigvals = np.linalg.eigvals( covmat)#/ autocovmat) # maximizing according to "When Slepian Meets Fidler: Putting a Focus on the Graph Spectrum", Van de Ville, Demesmaeker, Preti 2017 IEEE
                    eigvals = np.real( eigvals)
                    eigvals = eigvals/ np.nansum( eigvals)
                    #print( 'all eigenvalues: {}'.format( eigvals))
                    #sys.exit()
                    poseigind = np.argwhere( eigvals > np.nanmax( eigvals)*np.exp(-1.0)).flatten()
                    eigvals = eigvals[poseigind]
                    refeigvals = np.real( refeigvals)
                    refeigvals = refeigvals/ np.nansum( refeigvals)
                    refeigvals = refeigvals[poseigind]
                if( self.debug):
                    print( 'positive eigenvalues: {}'.format( eigvals))
                #condition = mymat > bound
                #print( condition)
                n = len(eigvals)
                self.NumOfSlep = n
                
                #print( np.shape( reference))
                #print( np.shape( dummy))
                Econcent = np.nansum( eigvals* eigvals)/ np.nansum( refeigvals* refeigvals)
                n = int( np.ceil( float( n) * myscale))
                #Econcent = np.matmul( reference, dummy.T)
                print( oldEconcent)
                print( Econcent)
                print( 'Deriving number of slepian windows: {}'.format( self.NumOfSlep))
                #sys.exit()
                #time.sleep(3)
            #self.NumOfSlep = len(eigvals)
            print( 'Dynamically derived number of slepian windows: {}'.format( self.NumOfSlep))
            #sys.exit()
        if( self.NumOfSlep == 0):
            self.NumOfSlep = self.NumOfSlep + 1
        #print( 'self.dt', self.dt)
        #print( 'self.len', self.len)
        
        if( self.debug):
            print( '[N, M]', [N, M])
            print( 'self.axis', self.axis)
            print( 'twoNW', twoNW)
            print( 'self.NumOfSlep', self.NumOfSlep)
        
        slep = dpss( [N, M][self.axis], twoNW, self.NumOfSlep, norm = 'approximate')
        #if( self.axis == 1):
        #    slep = slep.T
        if( self.debug):
            print( 'shape of slep is: {}'.format( np.shape( slep)))
        fdata = np.zeros( ( N, M), dtype = complex)
        #!!! Slepian dimension Problem !!!
        #scale = self.AmplitudeScale #np.sqrt( self.fmin* self.dt**2.0)
        #scale = np.sqrt( self.fmin/ self.dt)
        #print( 'scale', scale)
        #sys.exit()
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
                fdata[:, k] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis)) * self.AmplitudeScale, axis = 1)
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
                fdata[k, :] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis)) * self.AmplitudeScale, axis = 0).T
            #print( 'shape of fData in MySlepianApproach is:\n\t{}'.format( np.shape( fData)))
            #sys.exit()
        self.fdataMag = fft.fftshift( fdata, axes = self.axis)
        #self.fdataMag = fdata.T
        if( self.debug):
            print( 'shape of self.fdataMag is:\t{}'.format( np.shape( self.fdataMag)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            if( self.axis == 0):
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[self.posind, :]))
            elif( self.axis == 1):
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[:, self.posind]).T)
        #if( self.debug and self.axis == 1):
        #    print( 'shape of self.fdataMag is:\t{}'.format( np.shape( self.fdataMag)))
        #    print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
        #    plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[:, self.posind].T))
        #    #plt.plot( self.faxis, np.abs( self.fdataMag))
        if( self.debug):
            plt.grid(True)
            plt.show()
        return #self.faxis, self.fdataMag
    
    
    
    def __MySlepianApproachAng__( self):
        
        from scipy.signal.windows import dpss
        
        #fmin = 1.0/ np.max( np.shape( self.data))/ self.dt
        if( self.despike):
            print( 'Despiking data...')
            self.data, changeind = self.DeSpike()
        if( self.debug):
            print( 'shape of self.t is:\t{}'.format( np.shape( self.t)))
        if( self.aug):
            """ if there is some field augmentation applied"""
            #self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.FIELDAUGMENT( self.t, self.data)
            self.avg = np.nanmean( self.data, axis = self.axis)
            if( self.axis == 0):
                self.FIELDAUGMENT( self.t, self.data - self.avg)
            if( self.axis == 1):
                self.FIELDAUGMENT( self.t, self.data.T - self.avg)
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
        #!!! Slepian dimension Problem !!!
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
        """
        if( self.aug):
            " if there is some field augmentation applied"
            #self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.FIELDAUGMENT( self.t, self.data)
            self.avg = np.nanmean( self.data)
            self.FIELDAUGMENT( self.t, self.data - self.avg)
            if( self.debug):
                #n = 0
                for al, el in zip( self.augdata.T, self.data.T):
                    plt.plot( self.augt, al, 'r')
                    plt.plot( self.t, el - np.nanmean( el), 'g')
                
                plt.show()
        else:
            " if there is no field augmentation applied"
            self.augt, self.augdata, self.orgStartInd, self.orgEndInd = self.t, self.data, 0, self.shape[self.axis] - 1
        if( self.debug):
            print( 'shape of self.augt is:\t{}'.format( np.shape( self.augt)))
            print( 'shape of self.augdata is:\t{}'.format( np.shape( self.augdata)))
            print( 'self.orgStartInd is:\t{}'.format( self.orgStartInd))
            print( 'self.orgEndInd is:\t{}'.format( self.orgEndInd))
        dump = self.augdata
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
        self.__Ret2OldLen__()
        return self.faxis, self.fdata
    
    
    
    def fdataMag( self):
        self.__MySlepianApproachMag__()
        if( self.debug):
            plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[:,self.posind]).T)
            plt.show()
        self.__Ret2OldLen__()
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
    
    """
    THE NKEND
    """