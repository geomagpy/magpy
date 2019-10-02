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
import scipy.signal as sg
import scipy.fftpack as fft
import scipy.interpolate as ipol
from itertools import chain

debug = False # Print every single output

import matplotlib.pyplot as plt
print( 'pyplot imported as plt for debugging purposes')

class FrqFilter:
    """
    FrqFilter, V1.0alpha
    
    
    Reading 2D numpy.ndarray of "data" with equidistantly sampled timestamps in
    zerotime format and derives FFT of "data" ( "fdata"). Maybe some 
    sophisticated field-augmentation to suppress leakaging by using option 
    aug = True is applied. Anyway, aliasing may still be present in the "data".
    
    Suppression of leakage is based on an idea by Ramon Egli, ZAMG-Austria.
    
    
    
    Addtional an average noise level for spectra can be calculated using
    GetNoiseLvl() and the filtered spectra as well as the filtered timeseries'
    using filter()
    
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
        
        
        
        filter(): --- under construction ---
            Derives the filtered spectrum as well as the filtered timeseries
            and returns the filtered "faxis", "fdata", "filter" and "filtdata"
        
        
        
        GetNoiseLvl(OPTIONAL:{data = fdata,  perc = perc}):
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
        
        filter:  --- under construction ---
            numpy.array
            Array of window for filtering for twosided FFT (neg. and pos. 
            frequencies)
        
        filtdata:  --- under construction ---
            numpy.array
            Array of filtered timeseries'
        
        GetNoiseLvl:
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
                            lbub = ( lb, ub, de), kind = 'pass')
        
        faxis, fdata, filter, filtdata = FrqCont.filter()
        
        
        
        FrqCont = FrqFilter(data = data, axis = 0, 
                            dt = 10.0, aug = True, 
                            exp = ( ce, de), kind = 'block')
        
        faxis, fdata, filter, filtdata = FrqCont.filter()
        
        
        
        NoiseLvl = FrqCont.GetNoiseLevel(data = fdata, perc = 0.03)
        
        
        
        FrqCont = FrqFilter( data = data, axis = myaxis, dt = dt, 
                            rplval = 'linear', lvl = mylvl, despike = True, 
                            despikeperc = 0.01)
        
        despikedData, chgind = FrqCont.DeSpike()
    
    
    
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
                    self.t = np.arange( 0, float( self.len)* self.dt, self.dt)
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
                self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
                if( self.debug):
                    print( 'self.faxis is: {}'.format( self.faxis))
                self.posind = np.argwhere( self.faxis >= self.fmin).flatten()#[0]
                self.negind = np.argwhere( self.faxis <= -self.fmin).flatten()#[0]
                if( self.debug):
                    print( 'self.faxis, self.posind, self.negind are: {}, {}, {}'.format( self.faxis, self.posind, self.negind))
        except Exception as ex:
            """
            
            
            If everything goes wrong
            """
            print( 'Somethings wrong, exception is: {}'.format( ex))
            raise self.MyException(ex)
            #What to do if invalid parameters are passed
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
                                self.t = np.arange( 0, float( self.len)* self.dt, self.dt)
                                if( self.debug):
                                    print( 'self.t is: {}'.format( self.t))
                    except:
                        raise self.MyException( 'Got no timestamp for dataset...stopping!')
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
        N, M = np.shape( self.data)
        if( debug):
            print('shape of self.data is: {}x{}'.format( N, M))
        Data = np.diff( np.diff( self.data, axis = self.axis), axis = self.axis)
        if( self.axis == 0):
            #Data = self.data.T
            if( debug):
                print('shape of Data after np.diff is: {}'.format( np.shape( Data)))
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
            Data = np.vstack( ( Data.T, Data[:,-1])).T
            Data = np.vstack( ( Data[:,0], Data.T)).T
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
                for el in Data:
                    plt.plot( time, el)
            except:
                for el in Data.T:
                    plt.plot( time, el)
            plt.show()
        if( self.debug):
            print( 'Data is:\t{}\ntime is:\t{}\nrplval is:\t{}\nlvl is:\t{}\n'.format( Data, time, rplval, lvl))
            print( 'shape of Data is:\t{}\nshape of time is:\t{}\n'.format( np.shape( Data), np.shape( time)))
            #sys.exit()
        
        #chkavgs = np.nanmean( chkData, axis = self.axis)
        avgs = np.nanmedian( self.data, axis = self.axis)
        navgs = np.zeros( np.shape( avgs))
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
                        for k, ind in enumerate( mygoodind):
                            if( len( ind) > 0):
                                #goodind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in newchgind[k]]
                                #print( 'average of goodind[{}] is:\n\t{}'.format( k, np.mean( d[ goodind])))
                                goodind = ind
                            else:
                                goodind = np.arange( 0, [N, M][self.axis])
                            if( self.debug):
                                print( 'shape of goodind: {}'.format( np.shape( goodind)))
                            
                            badind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in goodind]
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
                        for k, ind in enumerate( mygoodind):
                            if( len( ind) > 0):
                                #goodind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in newchgind[k]]
                                #print( 'average of goodind[{}] is:\n\t{}'.format( k, np.mean( d[ goodind])))
                                goodind = ind
                            else:
                                goodind = np.arange( 0, [N, M][self.axis])
                            badind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in goodind]
                            #print tplind[0]
                            #print tplind[1]
                            #Data[ k, badind] = np.nan
                            chgData[ k, badind] = np.nan
                            sumbadind.append( badind)
                    
                    
                    
                    elif( str( rplval).startswith( 'cubic') or str( rplval).startswith( 'linear')):
                        temp = np.zeros( np.shape( chgData))
                        for k, (d, ind) in enumerate( zip( chgData, mygoodind)):
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
                            badind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in goodind]
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
                    for k, ind in enumerate( mygoodind):
                        if( len( ind) > 0):
                            goodind = ind
                        else:
                            goodind = np.arange( 0, [N, M][self.axis])
                        badind = [f for f in np.arange( 0, [N, M][self.axis]) if f not in goodind]
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
            self.despdata = chgData.T
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
            length = [ M, N][maxDim]
            columns = [ M, N][minDim]
            if( self.debug):
                print( 'Dimensions of aseries: \n M, N:{},{}'.format( M, N ))
                print( 'Dimensions of aseries: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
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
                print( 'three_length is: {}'.format( three_length))
            if( False):
                # only for debugging
                import matplotlib.pyplot as plt 
                import sys
                k = 2
                plt.plot( atseries, (three_aseries.T - nanmean( three_aseries, axis = 1))[:,k], 'r')
                plt.plot( tseries, (aseries.T - nanmean( aseries, axis = 1))[:,k], 'g')
                plt.show()
                sys.exit()
            
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
            orgStartInd = int( length) - ( firsthalfremlen + 2)
            #orgStartInd, orgEndInd = int( length) - ( firsthalfremlen + 2), int( cutlen - 1) - ( firsthalfremlen) - 1 # REMEMBER indices of original timestamps tseries[0] and tseries[-1] in atseries 
            orgEndInd = orgStartInd + length # REMEMBER indices of original timestamps tseries[0] and tseries[-1] in atseries 
            #orgStartInd, orgEndInd = int( length) - ( firsthalfremlen + 1), int( 2* length -1) - ( firsthalfremlen) # REMEMBER indices of original timestamps tseries[0] and tseries[-1] in atseries 
            if( self.debug):
                print( 'orgStartind is:\n\t{}\norgEndInd is:\n\t{}'.format( orgStartInd, orgEndInd))
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
            taper = parzen( taplen)
            maxind = argmax( taper)
            halftplen = int( floor( max( shape( taper))/2.0))
            
            #######
            # REMOVE TREND AT BEGINING
            #######
            trendind = [0, orgStartInd]
            if( self.debug):
                print( 'trendind is:\t{}'.format( trendind))
                print( 'shape of aaseries is:\t{}'.format( np.shape( aaseries)))
            trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
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
            trendind = [ orgEndInd - 2, pow2_length - 1]
            if( self.debug):
                print( 'orgEndInd:{}\npow2_length:{}\nM:{}'.format( orgEndInd, pow2_length, M))
            #trendind = [ orgEndInd -3, pow2_length - 1]
            trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
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
            #try:
            #    aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[maxind + 1::] # TAPERING
            #except:
            #    aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[maxind + 2::] # TAPERING
            #try:
            aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[-(trendind[-1] + 1 - trendind[0])::] # TAPERING
            #except:
            #    aaseries[:,trendind[0]:trendind[-1] + 1] = aaseries[:,trendind[0]:trendind[-1] + 1] * taper[maxind + 2::] # TAPERING
    
            
            #aaseries = (aaseries.T + avg_int_lvls).T
            
            
            if( False):
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
            if( self.debug):
                print( 'shape of self.augt is:\t{}'.format( shape( self.augt)))
                print( 'shape of self.augdata is:\t{}'.format( shape( self.augdata)))
                print( 'self.orgStartInd is:\t{}'.format( self.orgStartInd))
                print( 'self.orgEndInd is:\t{}'.format( self.orgEndInd))
            return atseries, aaseries.T, orgStartInd, orgEndInd
        
        """
        If the FIELDAUGMENTATION is callled as an argument of another function an artificial timeseries with sampling interval of
        1.0 second will be used
        """
        
        if( len( args) == 1):
            
            
            aseries = args[0]
            
            M, N = shape(aseries)
            minDim, maxDim = [ argmin( [M, N]), argmax( [M, N])]
            length = [ M, N][maxDim]
            columns = [ M, N][minDim]
            print( 'Dimensions of aseries: \n M, N:{},{}'.format( M, N ))
            print( 'Dimensions of aseries: \n minDim, maxDim:{},{}'.format( minDim, maxDim ))
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
            
            if( False):
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
            taplen =  orgStartInd + ( max( shape( aaseries)) - orgEndInd + 1) + 2
            if( self.debug):
                print( 'taplen is: {}'.format( taplen))
            tauval = float( taplen)/2.0 * 8.69/ D # https://en.wikipedia.org/wiki/Window_function from Exponential or Poisson window
            #taper = exponential( taplen, tau = tauval)
            taper = parzen( taplen)
            maxind = argmax( taper)
            halftplen = int( floor( max( shape( taper))/2.0))
            
            #######
            # REMOVE TREND AT BEGINING
            #######
            trendind = [0, orgStartInd]
            trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
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
            trendind = [ orgEndInd - 2, pow2_length - 1]
            #trendind = [ orgEndInd -3, pow2_length - 1]
            trendfct = zeros( ( M, 1 + trendind[-1] - trendind[0]))
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
            
            
            if( False):
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
        self.taper = sg.windows.hanning( self.len)
        if( self.data.ndim == 2):
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
        self.fdata = fft.fft( dummy, axis = self.axis) * np.sqrt( self.fmin)
        self.fdata = fft.fftshift( self.fdata, axes = self.axis)
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
            self.avg = np.nanmean( self.data, axis = self.axis)
            if( self.axis == 0):
                self.FIELDAUGMENT( self.t, self.data - self.avg)
            if( self.axis == 1):
                self.FIELDAUGMENT( self.t, self.data.T - self.avg)
                self.augdata = self.augdata.T
            
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
        if( np.shape( dump)[0] == np.shape( self.data)[1] or np.shape( dump)[1] == np.shape( self.data)[0]):
            self.augdata = dump.T
            dump = self.augdata
        print( 'shape of dump: {}'.format( np.shape( dump)))
        """
        
        derive frequency axis cause field-augmentation has to be concerned
        
        """
        
        self.len = np.shape( self.augt)[0]
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.fmin = 1.0/ float( self.len)/ self.dt
        if( self.PeriodAxis):
            #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
            self.posind = np.argwhere( self.faxis >= 1.0/ self.fnyq).flatten()
            self.negind = np.argwhere( self.faxis <= -1.0/ self.fnyq).flatten()
        else:
            #self.faxis = np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
            self.posind = np.argwhere( self.faxis >= self.fmin).flatten()
            self.negind = np.argwhere( self.faxis <= self.fmin).flatten()
        N, M = np.shape( dump)
        print( 'Length for dpss: {}'.format( [N, M][self.axis]))
        #slep = dpss( [N, M][self.axis], 5, self.NumOfSlep, norm = 2)
        slep = dpss( [N, M][self.axis], 5, self.NumOfSlep, norm = 'approximate')
        #if( self.axis == 1):
        #    slep = slep.T
        if( self.debug):
            print( 'shape of slep is: {}'.format( np.shape( slep)))
        fdata = np.zeros( ( N, M), dtype = complex)
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
                fdata[:, k] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis) * np.sqrt( self.fmin)), axis = 1)
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
                fdata[k, :] = np.nanmean( np.abs( fft.fft( dummy, axis = self.axis) * np.sqrt( self.fmin)), axis = 0).T
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
                self.augdata = self.augdata.T
            
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
        if( np.shape( dump)[0] == np.shape( self.data)[1] or np.shape( dump)[1] == np.shape( self.data)[0]):
            self.augdata = dump.T
            dump = self.augdata
        print( 'shape of dump: {}'.format( np.shape( dump)))
        """
        
        derive frequency axis cause field-augmentation has to be concerned
        
        """
        
        self.len = np.shape( self.augt)[0]
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.fmin = 1.0/ float( self.len)/ self.dt
        if( self.PeriodAxis):
            #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
            self.posind = np.argwhere( self.faxis >= 1.0/ self.fnyq).flatten()#[0]
            self.negind = np.argwhere( self.faxis <= -1.0/ self.fnyq).flatten()#[0]
        else:
            #self.faxis = np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
            self.posind = np.argwhere( self.faxis >= self.fmin).flatten()#[0]
            self.negind = np.argwhere( self.faxis <= self.fmin).flatten()#[0]
        N, M = np.shape( dump)
        print( 'Length for dpss: {}'.format( [N, M][self.axis]))
        #slep = dpss( [N, M][self.axis], 5, self.NumOfSlep, norm = 2)
        slep = dpss( [N, M][self.axis], 5, self.NumOfSlep, norm = 'approximate')
        #if( self.axis == 1):
        #    slep = slep.T
        if( self.debug):
            print( 'shape of slep is: {}'.format( np.shape( slep)))
        fdata = np.zeros( ( N, M))
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
                fdata[:, k] = np.nanmean( np.angle( fft.fft( dummy, axis = self.axis)), axis = 1)
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
                fdata[k, :] = np.nanmean( np.angle( fft.fft( dummy, axis = self.axis)), axis = 0).T
            #print( 'shape of fData in MySlepianApproach is:\n\t{}'.format( np.shape( fData)))
            #sys.exit()
        self.fdataAng = fft.fftshift( fdata, axes = self.axis)
        #self.fdataMag = fdata.T
        if( self.debug):
            print( 'shape of self.fdataAng is:\t{}'.format( np.shape( self.fdataAng)))
            print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
            if( self.axis == 0):
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataAng[self.posind, :]))
            elif( self.axis == 1):
                plt.loglog( self.faxis[self.posind], np.abs( self.fdataAng[:, self.posind]).T)
        #if( self.debug and self.axis == 1):
        #    print( 'shape of self.fdataMag is:\t{}'.format( np.shape( self.fdataMag)))
        #    print( 'shape of self.faxis is:\t{}'.format( np.shape( self.faxis)))
        #    plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[:, self.posind].T))
        #    #plt.plot( self.faxis, np.abs( self.fdataMag))
        if( self.debug):
            plt.grid(True)
            plt.show()
        return #self.faxis, self.fdataMag
    
    
    
    def __expfilt__( self):
        
        #from scipy.signal.windows import dpss
        
        #fmin = 1.0/ np.max( np.shape( self.data))/ self.dt
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
        self.len = np.shape( self.augt)[0]
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.fmin = 1.0/ float( self.len)/ self.dt
        if( self.PeriodAxis):
            #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
            self.posind = np.argwhere( self.faxis >= 1.0/ self.fnyq).flatten()#[0]
            self.negind = np.argwhere( self.faxis <= -1.0/ self.fnyq).flatten()#[0]
        else:
            #self.faxis = np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
            self.posind = np.argwhere( self.faxis >= self.fmin)#[0]
            self.negind = np.argwhere( self.faxis <= self.fmin)#[0]
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
        absfdata = np.abs( self.fdata)
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
            plt.loglog( self.faxis[self.posind], np.abs( self.filtfdata[self.posind,:]))
            plt.grid(True)
            plt.show()
        self.filtdata = np.real( fft.ifft( fft.ifftshift( self.filtfdata, axes = self.axis), axis = self.axis))[self.orgStartInd:self.orgEndInd, :]
        #self.filtdata = np.real( fft.ifft( self.filtfdata))[self.orgStartInd:self.orgEndInd, :]
        return self.augt[self.orgStartInd:self.orgEndInd], self.filtdata
    
    
    
    def __lbubfilt__( self): # --- under construction ---
        
        #from scipy.signal.windows import dpss
        
        #fmin = 1.0/ np.max( np.shape( self.data))/ self.dt
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
        self.len = np.shape( self.augt)[self.axis]
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.fmin = 1.0/ float( self.len)/ self.dt
        if( self.PeriodAxis):
            #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
            self.posind = np.argwhere( self.faxis >= 1.0/ self.fnyq).flatten()#[0]
            self.negind = np.argwhere( self.faxis <= -1.0/ self.fnyq).flatten()#[0]
        else:
            #self.faxis = np.hstack( ( np.arange( -self.fnyq - self.fmin,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
            self.posind = np.argwhere( self.faxis >= self.fmin).flatten()#[0]
            self.negind = np.argwhere( self.faxis <= self.fmin).flatten()#[0]
        if( self.debug):
            print( 'self.lb is :\t{}'.format( self.lb))
            print( 'self.ub is :\t{}'.format( self.ub))
        ######
        # pos frequencies
        ######
        self.lbind = np.argmin( np.abs( self.faxis - self.lb))
        self.ubind = np.argmin( np.abs( self.faxis - self.ub))
        if( self.debug):
            print( 'self.lbind is:\t{}'.format( self.lbind))
            print( 'self.ubind is:\t{}'.format( self.ubind))
        if( self.dim > 1):
            N, M = np.shape( dump)
        else:
            N = np.shape( dump)
        if( False):
            w = np.hstack( ( np.zeros( ( self.lbind - 1, ) ), np.ones( ( self.ubind - self.lbind, ) ), np.zeros( ( N - self.ubind,))))
        D = np.exp( self.de) # dezibel decay over half of window length
        #w = sg.gaussian( upperind -lowerind, std = 3.0* np.nanstd( np.abs( np.nanmean( fData[:,lowerind:upperind:], axis = 0))))
        #w = np.power( sg.parzen( upperind -lowerind), 4.0)
        tauval = float( N)/2.0 * 8.69/ D # https://en.wikipedia.org/wiki/Window_function from Exponential or Poisson window
        #w = sg.exponential( upperind -lowerind, center = np.argmax( np.abs( fData[:,lowerind:upperind:])), tau = tauval, sym = False) # exponential window function https://en.wikipedia.org/wiki/Window_function
        w = ( sg.exponential( N, center = self.ubind, tau = tauval, sym = False)) # exponential window function https://en.wikipedia.org/wiki/Window_function
        wmaxind = np.argmax( w)
        upslope = w[wmaxind:]
        
        
        rect = np.ones( ( self.ubind - self.lbind, ))
        w = ( sg.exponential( N, center = self.lbind, tau = tauval, sym = False)) # exponential window function https://en.wikipedia.org/wiki/Window_function
        wmaxind = np.argmax( w)
        lwslope = w[0:wmaxind]
        
        poswin = np.hstack( ( lwslope, rect, upslope))
        
        
        ######
        # neg frequencies
        ######
        self.lbind = np.argmin( np.abs( self.faxis + self.lb))
        self.ubind = np.argmin( np.abs( self.faxis + self.ub))
        if( self.debug):
            print( 'self.lbind is:\t{}'.format( self.lbind))
            print( 'self.ubind is:\t{}'.format( self.ubind))
        w = ( sg.exponential( N, center = self.ubind, tau = tauval, sym = False)) # exponential window function https://en.wikipedia.org/wiki/Window_function
        wmaxind = np.argmax( w)
        lwslope = w[0:wmaxind]
        
        
        rect = np.ones( ( self.lbind - self.ubind, ))
        w = ( sg.exponential( N, center = self.lbind, tau = tauval, sym = False)) # exponential window function https://en.wikipedia.org/wiki/Window_function
        wmaxind = np.argmax( w)
        upslope = w[wmaxind:]
        
        negwin = np.hstack( ( lwslope, rect, upslope))
        
        w = poswin + negwin
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
        self.__myFFTproc__()
        if( self.debug):
            print( 'shape of self.filter is: {}'.format( np.shape( self.filter)))
            print( 'shape of self.fdata is: {}'.format( np.shape( self.fdata)))
        absfdata = np.abs( self.fdata)
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
            plt.loglog( self.faxis[self.posind], np.abs( self.filtfdata[self.posind,:]))
            plt.grid(True)
            plt.show()
        self.filtdata = np.real( fft.ifft( fft.ifftshift( self.filtfdata, axes = self.axis), axis = self.axis))[self.orgStartInd:self.orgEndInd, :]
        #self.filtdata = np.real( fft.ifft( self.filtfdata))[self.orgStartInd:self.orgEndInd, :]
        return #self.faxis, self.fdataMag
    
    
    
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
        maxleng = int( self.perc * float( N))
        if( maxleng < 100):
            raise self.MyException('data array to short with length {}'.format( maxleng))
        r = []
        #inc = 10
        inc = int( float( maxleng)/ 100.0) # get the hundret best values
        if( np.ndim( d) == 1):
            check = np.abs( d[::-1])[:maxleng]
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
        self.noise = np.sqrt( np.pi)* np.sqrt( np.mean( np.power( r, 2.0)))
        if( self.debug):
            print( 'self.noise is:\n{}'.format( self.noise))
            #sys.exit()
        return self.noise
    
    
    
    def __Ret2OldLen__(self):
        #print( '\nshape of self.faxis\t{}'.format( np.shape( self.faxis)))
        #print( '\nshape of self.fdata\t{}'.format( np.shape( self.fdata)))
        #print( '\nshape of self.fDataMag\t{}'.format( np.shape( self.fdataMag)))
        #print( '\nshape of self.fdataAng\t{}'.format( np.shape( self.fdataAng)))
        self.len = self.shape[self.axis]
        if( self.debug):
            print( 'self.len is: {}'.format( self.len))
        self.t = np.arange( 0, float( self.len)* self.dt, self.dt)
        if( self.debug):
            print( 'self.t is: {}'.format( self.t))
        self.fnyq = 1.0/ 2.0/ self.dt
        if( self.debug):
            print( 'self.fnyq is: {}'.format( self.fnyq))
        if( self.debug):
            print( 'self.dt is: {}'.format( self.dt))
        self.fmin = 1.0/ float( self.len)/ self.dt
        if( self.debug):
            print( 'self.fmin is: {}'.format( self.fmin))
        if( self.PeriodAxis):
            #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            #self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
            bakax = self.faxis
            self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
        else:
            #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            #self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
            bakax = self.faxis
            self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
        if( self.aug == True):
            if( self.axis == 0):
                for el in self.__dict__.keys():
                    #print( 'Checking length of {}'.format( el))
                    #####or el.startswith( 'faxis')
                    if( el.startswith( 'fdata') or el.startswith( 'filtf') or el.startswith( 'filte')):
                        if( self.debug):
                            varname = 'self.' + el
                            print( 'changing shape of {} from {}'.format( varname, np.shape( self.__dict__[el])))
                        if( self.__dict__[el].ndim > 1):
                            if( np.argmax( np.shape( self.data)) != np.argmax( np.shape( self.__dict__[el]))):
                                self.__dict__[el] = self.__dict__[el].T
                            self.__dict__[el] = ipol.interp1d( bakax, self.__dict__[el], axis = self.axis)(self.faxis)
                        else:
                            #self.__dict__[el] = self.__dict__[el]
                            self.__dict__[el] = ipol.interp1d( bakax, self.__dict__[el])(self.faxis)
                        #print( 'changed shape of {} to {}'.format( varname, np.shape( self.__dict__[el])))
            elif( self.axis == 1):
                for el in self.__dict__.keys():
                    #print( 'Checking length of {}'.format( el))
                    #####or el.startswith( 'faxis')
                    if( el.startswith( 'fdata') or el.startswith( 'filtf') or el.startswith( 'filte')):
                        if( self.debug):
                            varname = 'self.' + el
                            print( 'changing shape of {} from {}'.format( varname, np.shape( self.__dict__[el])))
                        if( self.__dict__[el].ndim > 1):
                            if( np.argmax( np.shape( self.data)) != np.argmax( np.shape( self.__dict__[el]))):
                                self.__dict__[el] = self.__dict__[el].T
                            self.__dict__[el] = ipol.interp1d( bakax, self.__dict__[el], axis = self.axis)(self.faxis)
                        else:
                            #self.__dict__[el] = self.__dict__[el]
                            #print( 'changing shape of {} from {}'.format( varname, np.shape( self.__dict__[el])))
                            self.__dict__[el] = ipol.interp1d( bakax, self.__dict__[el])(self.faxis)
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
                        plt.loglog( self.faxis, np.abs( self.__dict__[el]).T)
                    elif( self.axis == 1):
                        plt.loglog( self.faxis, np.abs( self.__dict__[el]).T)
            plt.grid(True)
            plt.show()
        #sys.exit()
        if( self.PeriodAxis):
            #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            #self.faxis = 1.0/ np.arange( -self.fnyq, self.fnyq, self.fmin)
            #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            #self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
            if( self.debug):
                print( 'self.faxis is: {}'.format( self.faxis))
            a = self.faxis >= 1.0/ self.fnyq
            b = self.faxis <= 1.0/ self.fmin
            c = self.faxis <= -1.0/ self.fnyq
            d = self.faxis >= -1.0/ self.fmin
            if( self.debug):
                print( '\na\t=\t{}\nb\t=\t{}\nc\t=\t{}\nd\t=\t{}'.format( a, b, c, d))
                print( '\nshapes: a\t=\t{}\nb\t=\t{}\nc\t=\t{}\nd\t=\t{}'.format( np.shape( a), np.shape( b), np.shape( c), np.shape( d)))
            if( self.debug):
                print( 'shape of augmented timeseries posind: {}'.format( np.shape( self.posind)))
            #self.posind = np.argwhere( a & b).flatten()#[0]
            #self.negind = np.argwhere( c & d).flatten()#[0]
            self.posind = np.argwhere( a & b).flatten()#[0]
            self.negind = np.argwhere( c & d).flatten()#[0]
            self.posind = np.argwhere( self.faxis >= 1.0/ self.fnyq).flatten()#[0]
            self.negind = np.argwhere( self.faxis <= -1.0/ self.fnyq).flatten()#[0]
            if( self.debug):
                print( 'self.faxis, self.posind, self.negind are: {}, {}, {}'.format( self.faxis, self.posind, self.negind))
        else:
            #self.faxis = 1.0/ np.hstack( ( np.arange( -self.fnyq,-self.fmin, self.fmin), np.arange( self.fmin, self.fnyq, self.fmin)))
            #self.faxis = np.arange( -self.fnyq, self.fnyq, self.fmin)
            if( self.debug):
                print( 'self.faxis is: {}'.format( self.faxis))
            a = self.faxis >= self.fmin
            b = self.faxis <= self.fnyq
            c = self.faxis <= -self.fmin
            d = self.faxis >= -self.fnyq
            if( self.debug):
                print( '\na\t=\t{}\nb\t=\t{}\nc\t=\t{}\nd\t=\t{}'.format( a, b, c, d))
                print( '\nshapes: a\t=\t{}\nb\t=\t{}\nc\t=\t{}\nd\t=\t{}'.format( np.shape( a), np.shape( b), np.shape( c), np.shape( d)))
            if( self.debug):
                print( 'shape of augmented timeseries posind: {}'.format( np.shape( self.posind)))
            #self.posind = np.argwhere( a & b).flatten()#[0]
            #self.negind = np.argwhere( c & d).flatten()#[0]
            self.posind = np.argwhere( a & b).flatten()#[0]
            self.negind = np.argwhere( c & d).flatten()#[0]
            if( self.debug):
                print( 'shape of original posind: {}'.format( np.shape( self.posind)))
            if( self.debug):
                print( 'self.faxis[self.posind]: {}'.format( self.faxis[self.posind]))
            if( self.debug):
                print( 'self.faxis, self.posind, self.negind are: {}, {}, {}'.format( self.faxis, self.posind, self.negind))
        return
    
    
    def fdata( self):
        self.__myFFTproc__()
        self.__Ret2OldLen__()
        return self.faxis, self.fdata
    
    
    
    def fdataMag( self):
        self.__MySlepianApproachMag__()
        plt.loglog( self.faxis[self.posind], np.abs( self.fdataMag[:,self.posind]).T)
        plt.show()
        self.__Ret2OldLen__()
        return self.faxis, self.fdataMag
    
    
    
    def fdataAng( self):
        self.__MySlepianApproachAng__()
        self.__Ret2OldLen__()
        return self.faxis, self.fdataAng
    
    
    
    def filter( self):
        if( self.type == 'exp'):
            self.__expfilt__()
        elif( self.type == 'lbub'):
            self.__lbubfilt__()
        self.__Ret2OldLen__()
        return self.faxis, self.filtfdata, self.filter, self.filtdata
    
    """
    THE NKEND
    """