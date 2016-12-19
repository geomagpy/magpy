#!/usr/bin/env python
'''
Path:                   magpy.opt.stormdet
Part of package:        stormdet
Type:                   Library of functions for storm detection

PURPOSE:
        Script containing all programs needed for storm detection algorithm.
        All functions called by seekStorm().
        Require PyWavelets package for DWT evaluations.
        Created by RLB on 2015-01-21.

CONTAINS:
    (MAIN...)
        seekStorm:      (Func) Mostly automated function that will seek a storm-
                        like signal in a DataStream object.
    (SUPPORTING...)
        checkACE:       (Func) Uses input ACE data to seek out CME shocks arriving
                        at the satellite.
        findSSC:        (Func) Uses data of the geomagnetic field to detect SSCs using
                        the DWT or FDM method.
        findSSC_AIC:    (Func) Uses data of the geomagnetic field to detect SSCs using
                        the AIC method.
    (OPTIONAL/INTERNAL FUNCTIONS...)
        _calcDVals:     (Func) ... internal function used by checkACE.
        _calcProbWithSat:(Func) ... internal function for probability calculations
                        used by findSSC and findSSC_AIC.

DEPENDENCIES:
        magpy.stream
        magpy.mpplot
        matplotlib
        pywt (PyWavelet)

CALLED BY:
        External analysis scripts only.

EXAMPLE SCRIPT:
        >>> from magpy.stream import *
        >>> from magpy.opt.stormdet import *
        >>> date = '2015-03-17'
        >>> magdata = read("FGE_"+date+".cdf")
        >>> sat_1m = read("ace_1m_"+date+".cdf")
        >>> sat_5m = read("ace_5m_"+date+".cdf")
        >>> magdata = magdata.smooth('x',window_len=15)
        >>> offsets = [20851.,0.,43453.]
        >>> magdata.offset({'x': offsets[0], 'y': offsets[1], 'z': offsets[2]})
        >>> magdata = magdata._convertstream('xyz2hdz')
        >>> detection, ssc_list = seekStorm(magdata, satdata_1m=sat_1m, satdata_5m=sat_5m,
                verbose=True, method='DWT3')
        >>> print(ssc_list)
'''
from __future__ import print_function

import sys, os
from magpy.stream import *
from magpy.mpplot import *

#--------------------------------------------------------------------
# VARIABLES
#--------------------------------------------------------------------

# Normal variables:
# Picked out using SD_VarOptimise on methods tested on all storms (vartests):
funcvars = {    'AIC':   [4.,    3.,     20],
                'DWT2':  [0.000645, 60], # <-- OFFICIAL 100% VALUES #[0.0045, 45], #
                'DWT1':  [2.4499e-05, 65], #[3.89999e-05, 60] (for 14)
                'MODWT': [0.0012, 65], #[3.89999e-05, 60] (for 14)
                'FDM':   [0.0005, 55],
                'JDM':   [11,    1]      }


# Minimum for storm detection:
d_amp_min = 5. # nT (WEAK)

# ACE Variables:
ace_window = 30 # 0.5x window of minutes around estimated arrival time for SSC

# Calculation of arrival time:
a_varr = -0.000064328748639 #-5.18166868e-5
b_varr =  0.0599247768 #0.0560048791


#####################################################################
#    MAIN FUNCTION:   seekStorm()                                   #
#####################################################################

def seekStorm(magdata, satdata_1m=None, satdata_5m=None, method='AIC', variables=None,
        magkey='x', dwt_level='db4', returnsat=False, plot_vars=False, verbose=False):
    '''
    DEFINITION:
        Main function. Input data and it will return a dictionary detailing if
        it found a storm onset and, if so, at what time and what strength.

    PARAMETERS:
    Variables:
        - magdata:      (DataStream) Stream of magnetic (1s) data.
    Kwargs:
        - satdata_1m:   (DataStream) Stream of 1m ACE swepam data.
        - satdata_5m:   (DataStream) Stream of 5m ACE epam data.
        - method:       (str) Evaluation function. There are three options:
                        'AIC': Akaike Information Criterion (default)
                        'DWT2': Discrete Wavelet Transform (2nd detail) - see Hafez et al. (2013)
                        'DWT1': Discrete Wavelet Transform (3rd detail)
                        'MODWT': Maximal Overlap Discrete Wavelet Transform
                        'FDM': First Derivative Method
        - variables:    (list) List of variables used in individual functions. Len = 2 (except AIC)
                        Defaults: funcvars = {
                                'AIC':  [4.,    3.,     20],
                                'DWT2': [0.000645, 60],
                                'DWT3': [2.4499e-05, 65],
                                'FDM':  [0.0005, 55],
                                'JDM':  [11,    1]      }
        - magkey:       (str) Key in magnetic data to evaluate. Default is x (H-component)
        - dwt_level:    (str) Type of filter for wavelet analysis. Default 4th-order Daubechies filter.
                        For more options consult pywt.wavedec() documentation.
        - returnsat:    (bool) If True, will return results from satellite evaluation
        - plot_vars:    (bool) If True, plots of variables will be shown during func call.
        - verbose:      (bool) If True, calculation steps will be printed.

    RETURNS:
        - detection:    (bool) If True, storm was detected
        - ssc_list:     (list[dict]) List of dictionaries, one for each storm in format:
                        'ssctime': time of storm detection (datetime.datetime object)
                        'amp': size of SSC in nT (float)
                        'duration': length of SSC in s (float)
                        'probf': probability that detection is real, 50-100 % (float)
                        (Only consider those with probf > 80!)

    EXAMPLE:
        >>> detection, ssc_list = seekStorm(magstream)
    '''

    if not variables:
        variables = funcvars[method]

    detection = False
    ssc_list = []

    if (satdata_1m and satdata_5m) != None:
        if verbose == True:
            print("Using ACE data!")
        useACE = True
    elif (satdata_1m or satdata_5m) != None:
        if verbose == True:
            print("Can't use ACE for seekStorm if only one data stream is present!")
        useACE = False
    else:
        if verbose == True:
            print("No ACE data available. Continuing without.")
        useACE = False

    # EVALUATE ACE SATELLITE DATA
    # ---------------------------
    if useACE:
        try:
            ACE_detection, ACE_results = checkACE(satdata_1m, ACE_5m=satdata_5m, verbose=verbose)
            if verbose == True:
                print("ACE data successfully evaluated.")
        except:
            if verbose == True:
                print("ACE evaluation failed. Continuing without.")
            useACE = False
            ACE_detection, ACE_results = False, []
    else:
        ACE_detection, ACE_results = False, []

    if ACE_detection == True:
        for d in ACE_results:
            if verbose == True:
                print("ACE SSC detected at %s with speed %s and flux %s!" % (d['satssctime'], d['vwind'], d['pflux']))
    else:
        if verbose == True:
            print("No ACE storm.")

    #day = datetime.strftime(num2date(magdata[10].time),'%Y-%m-%d')
    t_ind = KEYLIST.index('time')
    day = datetime.strftime(num2date(magdata.ndarray[t_ind][10]),'%Y-%m-%d')

    a, p = variables[0], variables[1]

    # AIC - AKAIKE INFORMATION CRITERION
    # ----------------------------------
    if method == 'AIC':
        AIC_key = 'var2'
        #AIC_newkey = 'var4'
        AIC_dkey = 'var3'
        trange = 30

        magdata = magdata.aic_calc('x',timerange=timedelta(minutes=trange),aic2key=AIC_key)
        magdata = magdata.differentiate(keys=[AIC_key],put2keys=[AIC_dkey])

        minlen = variables[2]
        detection, ssc_list = findSSC_AIC(magdata, AIC_key, AIC_dkey, a, p, minlen,
                useACE=useACE, ACE_results=ACE_results, verbose=verbose)
        if plot_vars == True:
            plot_new(magdata,['x',AIC_key,AIC_dkey])

    # DWT - DISCRETE WAVELET TRANSFORM
    # --------------------------------
    elif method == 'DWT2' or method == 'DWT1': # using D2 or D3 detail
        DWT = magdata.DWT_calc()
        if method == 'DWT2':
            var_key = 'var2'
        elif method == 'DWT1':
            var_key = 'var1'
        detection, ssc_list = findSSC(DWT, var_key, a, p, useACE=useACE, ACE_results=ACE_results, verbose=verbose)
        if plot_vars == True:
            plotStreams([magdata, DWT],[['x'],['dx','var1','var2','var3']],plottitle=day)

    # MODWT - MAXIMAL OVERLAP DISCRETE WAVELET TRANSFORM
    # --------------------------------------------------
    elif method == 'MODWT': # using D2 or D3 detail
        MODWT = magdata.MODWT_calc(level=1, wavelet='haar')
        var_key = 'var1'
        detection, ssc_list = findSSC(MODWT, var_key, a, p, useACE=useACE, ACE_results=ACE_results, verbose=verbose)
        if plot_vars == True:
            plotStreams([magdata, DWT],[['x'],['dx','var1']],plottitle=day)

    # FDM - FIRST DERIVATION METHOD
    # -----------------------------
    elif method == 'FDM':
        FDM_key = 'dx'
        magdata = magdata.differentiate(keys=['x'],put2keys=[FDM_key])
        magdata.multiply({FDM_key: 2}, square=True)
        detection, ssc_list = findSSC(magdata, FDM_key, a, p, useACE=useACE, ACE_results=ACE_results, verbose=verbose)
        if plot_vars == True:
            plot_new(magdata,['x','dx'])

    else:
        print("%s is an invalid evaluation function!" % method)

    if not returnsat:
        return detection, ssc_list
    else:
        return detection, ssc_list, ACE_results


#********************************************************************
#    SUPPORTING FUNCTIONS FOR seekStorm:                            #
#       checkACE()                                                  #
#       findSSC()                                                   #
#       findSSC_AIC()                                               #
#********************************************************************

def checkACE(ACE_1m,ACE_5m=None,acevars={'1m':'var2','5m':'var1'},timestep=20,lastcompare=20,
        vwind_bracket=[380., 450.], vwind_weight=1., dvwind_bracket=[40., 80.], dvwind_weight=1.,
        stdlo_bracket=[15., 7.5], stdlo_weight=1., stdhi_bracket=[0.5, 2.], stdhi_weight=1.,
        pflux_bracket=[10000., 50000.], pflux_weight=4., verbose=False):
    '''
    DEFINITION:
        This function picks out rough timings of behaviour in ACE measurements that
        represent the start of a geomagnetic storm, i.e. discontinuities. The
        likelihood that an event detected by this method is a storm is determined
        according to a variety of factors.

    PARAMETERS:
    Variables:
        - ACE_1m:       (DataStream) Stream of 1min ACE swepam data.
                        Must contain: bulk solar wind speed
    Kwargs:
        - ACE_5m:       (DataStream) Stream of 5min ACE epam data
                        Must contain: Proton flux 47-68
        - acevars:      (dict) Variables containing relevant stream keys for solar wind
                        ('1m' = swepam) and proton flux ('5m' = epam) in each stream.
                        Default: '1m':'var2','5m':'var1'
        - timestep:     (int) Number of minutes to use as timestep when looking for
                        storm onset. Default = 20 [mins]
        - lastcompare:  (int) Number of minutes to compare timestep to. Default = 20 [mins]
        - vwind_bracket:(list) Bracket defining limits for solar wind speed probability calc.
                        Default [380., 450.].
        - vwind_weight: (float) Weighting factor determining weight of vwind in total prob calc.
                        Default 1.
                        Together they work like this - the same is true for all bracket/weights:
                        if v_max < vwind_bracket[0]: v_prob = 50.
                        elif vwind_bracket[0] <= v_max < vwind_bracket[1]: v_prob = 75.
                        elif v_max > vwind_bracket[1]: v_prob = 100.
                        else: v_prob = 50.
                        v_prob = v_prob * vwind_weight
        - dvwind_bracket:(list) Bracket defining limits for change in solar wind speed
                        probability calc. Default [40., 80.].
        - dvwind_weight:(float) Weighting factor determining weight of change in vwind in
                        total prob calc. Default 1.
        - stdlo_bracket:(list) Bracket defining limits for stddev before CME probability
                        calc. Default [15., 7.5]
        - stdlo_weight: (float) Weighting factor determining weight of stddev before CME
                         in total prob calc. Default 1.
        - stdhi_bracket:(list) Bracket defining limits for stddev after > x*stddev before
                        probability calc. Default [0.5, 2.].
        - stdhi_weight: (float) Weighting factor determining weight of ... in total prob calc.
                        Default 1.
        - verbose:      (bool) If True, will print variables used in detection for
                        evaluation. Default = False
    RETURNS:
        - detection:    (bool) If True, storm was detected. False, no storm detected.
        - ssc_list:     (list[dict]) List of dictionaries, one for each storm in format:
                        'satssctime': time of detected CME arrival (datetime.datetime object)
                        'estssctime': estimated time of SSC on Earth (datetime.datetime object)
                        'vwind': solar wind speed at CME detection (float)
                        'pflux': proton flux at CME detection (float)
                        'probf': probability that detection is real, 50-100 % (float)
                        (Only consider those with probf > 75!)

    EXAMPLE:
        >>> ACE_det, ACE_ssc_list = checkACE(ACE_1m, ACE_5m=ACE_5m, verbose=True)
    '''

    # CHECK DATA:
    # -----------
    if len(ACE_1m) == 0:
        raise Exception("Empty 1min SWEPAM stream!")
    if ACE_5m != None:
        if len(ACE_5m) == 0:
            raise Exception("Empty 5min EPAM stream!")

    # DEFINE PARAMETERS:
    # ------------------
    test_det, detection = True, False
    key_s = acevars['1m']
    key_e = acevars['5m']
    ace_ssc = []

    t_ind = KEYLIST.index('time')
    nantest = ACE_1m._get_column(key_s)
    #nantest = ACE_1m.ndarray[s_ind]
    ar = [elem for elem in nantest if not isnan(elem)]
    div = float(len(ar))/float(len(nantest))*100.0
    if div < 10.:
        raise Exception("Too many NaNs in ACE SWEPAM data.")

    # CALCULATE VARIABLES FOR USE IN EVALUATION:
    # ------------------------------------------
    dACE = _calcDVals(ACE_1m, key_s, lastcompare, timestep)

    while True:

        # Change in mean values of v_sw:
        dv_test = dACE._get_column('dx')
        # Time array:
        t_test = dACE._get_column('time')
        # Average standard deviation of v at each point according to timerange:
        std_test = dACE._get_column('var4')
        # Average value of v at each point according to timerange:
        v_test = dACE._get_column('var2')

        # Check data is not empty:
        if len(dv_test) == 0:
            break

        # Check for large percentages of nans:
        ar = [elem for elem in dv_test if not isnan(elem)]
        div = float(len(ar))/float(len(dv_test))*100.0
        if div < 5.:
            if verbose == True:
                print("Too many nans!")
            break

        # Find max in change of mean (if dv_max > 25 --> detection!):
        i_max = np.nanargmax(dv_test)
        dv_max, t_max = dv_test[i_max], t_test[i_max]
        try:
            v_max = v_test[i_max+5]
            i = 1
            while True:
                if isnan(v_max):
                    v_max = v_test[i_max+5+i]
                else:
                    break
                #logger.info("checkACE: Too many v_wind NaNs... searching for value...")
                i += 1
        except:
            v_max = v_test[i_max]

        # CALCULATE PROB THAT DETECTION IS REAL STORM:
        # --------------------------------------------
        if dv_max >= 25.:

            #logger.info("checkACE: Found a possible detection...")
            t_lo = num2date(t_max) - timedelta(minutes=(timestep+20))
            t_hi = num2date(t_max) + timedelta(minutes=(lastcompare+20))
            dACE_lo = dACE.trim(starttime=t_lo, endtime=num2date(t_max), newway=True)
            dACE_hi = dACE.trim(starttime=num2date(t_max), endtime=t_hi, newway=True)

            std_lo = dACE_lo.mean('var4')
            std_hi = dACE_hi.mean('var4')

            # 1ST REQUIREMENT:
            # ****************
            # A storm is most likely at higher wind speeds (v > 450.)
            # Solar wind speed at max:
            if v_max < vwind_bracket[0]: v_prob = 50.
            elif vwind_bracket[0] <= v_max < vwind_bracket[1]: v_prob = 75.
            elif v_max > vwind_bracket[1]: v_prob = 100.
            else: v_prob = 50.
            v_prob = v_prob * vwind_weight

            # 2ND REQUIREMENT:
            # ****************
            # Must be a sudden jump (discontinuity) in the data.
            # Delta-v amplitude:
            if dv_max < dvwind_bracket[0]: dv_prob = 50.
            elif dvwind_bracket[0] <= dv_max < dvwind_bracket[1]: dv_prob = 75.
            elif dv_max > dvwind_bracket[1]: dv_prob = 100.
            else: dv_prob = 50.
            dv_prob = dv_prob * dvwind_weight

            # 3RD REQUIREMENT:
            # ****************
            # Check for a change in std. dev. after the discontinuity.
            # Std dev. (BEFORE discontinuity):
            if std_lo > stdlo_bracket[0]: stlo_prob = 50.
            elif stdlo_bracket[1] <= std_lo < stdlo_bracket[0]: stlo_prob = 75.
            elif std_lo < stdlo_bracket[1]: stlo_prob = 100.
            else: stlo_prob = 50.
            stlo_prob = stlo_prob * stdlo_weight

            # Std dev. (AFTER discontinuity):
            if std_hi < stdhi_bracket[0]*std_lo: sthi_prob = 50.
            elif stdhi_bracket[0]*std_lo <= std_hi < stdhi_bracket[1]*std_lo: sthi_prob = 75.
            elif std_hi >= stdhi_bracket[0]*std_lo: sthi_prob = 100.
            else: sthi_prob = 50.
            sthi_prob = sthi_prob * stdhi_weight

            # Calculate final probability:
            total_weight = vwind_weight + dvwind_weight + stdlo_weight + stdhi_weight
            probf = (v_prob + dv_prob + stlo_prob + sthi_prob)/total_weight

            if verbose == True:
                print(num2date(t_max), v_max, dv_max, std_lo, std_hi)
                print(num2date(t_max), v_prob, dv_prob, stlo_prob, sthi_prob, '=', probf)

            # Detection is likely a storm if total probability is >= 70.
            #if probf >= 70.:
            # Check ACE EPAM data for rise in solar proton flux
            if ACE_5m != None:
                t_5m = ACE_5m._get_column('time')
                t_val, idx = find_nearest(t_5m,t_max)
                start, end = (num2date(t_val),
                        num2date(t_val)+timedelta(minutes=10))
                # TODO: Figure out why this function still decimates ACE_5m
                ACE_flux = ACE_5m.trim(starttime=start,endtime=end, newway=True)
                flux_val = ACE_flux.mean(key_e,percentage=20)
                if isnan(flux_val):
                    # First try larger area:
                    ACE_flux = ACE_5m.trim(starttime=start-timedelta(minutes=10),
                        endtime=(end+timedelta(minutes=20)), newway=True)
                    flux_val = ACE_flux.mean(key_e,percentage=20)
                    if isnan(flux_val):
                        #logger.warning("checkACE: Proton Flux is nan!", flux_val)
                        flux_val = 0.
                        #raise Exception
                #logger.info("--> Jump in solar wind speed detected! Proton flux at %s." % flux_val)

                # FINAL REQUIREMENT:
                # ******************
                # Proton flux (47-68 keV) must exceed 10000. Often it reaches above 200000 during
                # a storm, but quiet time values rarely exceed 10000.
                if flux_val < pflux_bracket[0]:
                    pflux_prob = 0.
                    if verbose == True:
                        print("That's not really a storm!")
                        print(ace_ssc)
                elif pflux_bracket[0] <= flux_val < pflux_bracket[1]:
                    pflux_prob = 50.
                    if verbose == True:
                        print("That's almost a storm!")
                        print(ace_ssc)
                elif flux_val >= pflux_bracket[1]:
                    pflux_prob = 100.
                    if verbose == True:
                        print("That's almost a storm!")
                        print(ace_ssc)
                pflux_prob = pflux_prob * pflux_weight
                probf = probf * total_weight
                probf = (probf + pflux_prob)/(total_weight + pflux_weight)
                flux_var = flux_val
                
            elif ACE_5m == None:
                flux_var = 0.

            acedict = {}
            # CALCULATE ESTIMATED ARRIVAL TIME
            satssctime = num2date(t_max).replace(tzinfo=None)
            #a_arr = -0.0000706504807969411
            #b_arr = 0.0622525912
            #arr_est = a_arr * v_max + b_arr # original
            #estssctime = satssctime + timedelta(minutes=(arr_est * 60. * 24.))
            a_arr = 2.29684514e+04
            b_arr = -7.62595734e+00
            arr_est = a_arr * (1./v_max) + b_arr
            estssctime = satssctime + timedelta(minutes=arr_est)
            acedict['satssctime'] = satssctime
            acedict['estssctime'] = estssctime
            acedict['vwind'] = v_max
            acedict['pflux'] = flux_var
            acedict['probf'] = probf
            detection = True
            ace_ssc.append(acedict)

            if verbose:
                print("Removing data from %s to %s." % (num2date(t_max)-timedelta(minutes=15+timestep), num2date(t_max)+timedelta(minutes=15+lastcompare)))
            dACE = dACE.remove(starttime=num2date(t_max)-timedelta(minutes=15+timestep),
                        endtime=num2date(t_max)+timedelta(minutes=15+lastcompare))

        else:
            break

    # Until boundary effects can be mitigated, this should deal with potentially incorrect detections:
    # TODO: take this out at some point
    if detection:
        if ((num2date(ACE_1m.ndarray[t_ind][-1]).replace(tzinfo=None) - ace_ssc[0]['satssctime']).seconds < (timestep/2.)*60. or
                (ace_ssc[0]['satssctime'] - num2date(ACE_1m.ndarray[t_ind][0]).replace(tzinfo=None)).seconds < (timestep/2.)*60.):
            detection, ace_ssc = False, []

    if verbose == True:
        print("ACE storms detected at:")
        for item in ace_ssc:
            print(item['satssctime'])

    return detection, ace_ssc


def findSSC(var_stream, var_key, a, p, useACE=False, ACE_results=[], dh_bracket=[5.,10.],
        satprob_weight=1., dh_weight=1., estt_weight=2., verbose=False, ):
    '''
    DEFINITION:
        This function works on data evaluated using either a Discrete Wavelet
        Transform (DWT) or a First Derivative (FDM). It takes a stream and will
        extract a maximum in that stream that exceeds the amplitude threshhold
        a for the period p.

    PARAMETERS:
    Variables:
        - var_stream:   (DataStream) Stream containing data to be analysed (e.g. DWT stream)
        - var_key:      (str) Key of variable to be analysed in stream
        - a:            (float) Minimum amplitude threshold for peak
        - p:            (float) Minimum duration peak must exceed a
    Kwargs:
        - useACE:       (bool) If True, ACE results will be implemented in detection as a
                        further criterium. Require ACE_results != []. Default = False
        - ACE_results:  (list[dict]) Results in format returned from checkACE function. Should
                        be a list of SSCs in ACE data. Default = None
        - dh_bracket:   (list) Bracket definining probabilities of detections with dh
                        within certain ranges.
        - satprob_weight:(float) Weight applied to original CME detection prob in final prob
                        calc. Default 1.
        - dh_weight:    (float) Weight applied to dH of SSC prob in final prob calculation.
                        Default 1.
        - estt_weight:  (float) Weight applied to probability calculated from arrival time in
                        relation to estimated arrival time in final prob calculation. Default 2.
        - verbose:      (bool) If True, will print variables used in detection for
                        evaluation. Default = False

    RETURNS:
        - detection:    (bool) If True, storm was detected
        - ssc_list:     (list[dict]) List of dictionaries, one for each storm in format:
                        'ssctime': time of storm detection (datetime.datetime object)
                        'amp': size of SSC in nT (float)
                        'duration': length of SSC in s (float)
                        'probf': probability that detection is real, 50-100 % (float)
                        (Only consider those with probf > 80!)

    EXAMPLE:
        >>> magdata = read(FGE_file)
        >>> satdata_1m = read(ACE_1m_file)
        >>> satdata_5m = read(ACE_5m_file)
        >>> ACE_detection, ACE_results = checkACE(satdata_1m, satdata_5m)
        >>> DWT = magdata.DWT_calc()
        >>> var_key = 'var2'            # use second detail D2
        >>> a, p = 0.0004, 45           # amplitude 0.0004 in D2 var must be exceeded over period 45 seconds
        >>> detection, ssc_list = findSSC(DWT, var_key, a, p, useACE=useACE,
                ACE_results=ACE_results, verbose=verbose)
    '''

    # CHECK DATA:
    # -----------
    if len(var_stream) == 0:
        raise Exception("Empty stream!")

    possdet = False
    detection = False
    SSC_list = []

    var_ind = KEYLIST.index(var_key)
    var_ar = var_stream.ndarray[var_ind]
    t_ind = KEYLIST.index('time')
    t_ar = var_stream.ndarray[t_ind]
    x_ind = KEYLIST.index('x')
    x_ar = var_stream.ndarray[x_ind]

    i = 0

    # SEARCH FOR PEAK:
    # ----------------
    if verbose == True:
        print("Starting analysis with findSSC().")
    #for row in var_stream:
    for i in range(0,len(var_ar)):
        #var = eval('row.'+var_key)
        var = var_ar[i]

        # CRITERION #1: Variable must exceed the threshold a
        # **************************************************
        if var >= a and possdet == False:
            #timepin = row.time
            timepin = t_ar[i]
            #x1 = row.x
            x1 = x_ar[i]
            possdet = True
        elif var < a and possdet == True:
            #duration = (num2date(row.time) - num2date(timepin)).seconds
            duration = (num2date(t_ar[i]) - num2date(timepin)).seconds

            # CRITERION #2: Length of time that variable exceeds a must > p
            # *************************************************************
            if duration >= p:
                #x2 = row.x
                x2 = x_ar[i]
                d_amp = x2 - x1
                ssc_init = num2date(timepin).replace(tzinfo=None)
                if verbose == True:
                    print("x1:", x1, ssc_init)
                    #print "x2:", x2, num2date(row.time)
                    print("x2:", x2, num2date(t_ar[i]))
                    print("Possible detection with duration %s at %s with %s nT." % (duration, ssc_init, d_amp))

                # CRITERION #3: Variation in H must exceed a certain value
                # ********************************************************
                if d_amp > d_amp_min:

                    if d_amp >= dh_bracket[0] and d_amp < dh_bracket[1]:
                        dh_prob = 50.
                    if d_amp >= dh_bracket[1]:
                        dh_prob = 100.

                    if verbose == True:
                        print("Detection at %s with %s nT!" % (ssc_init,str(d_amp)))

                    # CRITERION #4: Storm must have been detected in ACE data
                    # *******************************************************
                    if useACE == True and ACE_results != []:

                        # CRITERION #5: ACE storm must have occured 45 (+-20) min before detection
                        # ************************************************************************
                        for sat_ssc in ACE_results:
                            det, final_probf = _calcProbWithSat(ssc_init, sat_ssc,
                                dh_prob, dh_weight, satprob_weight, estt_weight, verbose=verbose)
                            if det == True:
                                break

                    elif useACE == True and ACE_results == []:
                        detection, det = False, False
                        if verbose == True:
                            print("No ACE storm. False detection!")
                            print(ssc_init, d_amp)

                    elif useACE == False:
                        detection, det = True, True
                        final_probf = dh_prob
                        if verbose == True:
                            print("No ACE data. Not sure!")

                    if det == True:
                        SSC_dict = {}
                        SSC_dict['ssctime'] = ssc_init
                        SSC_dict['amp'] = d_amp
                        SSC_dict['duration'] = duration
                        SSC_dict['probf'] = final_probf
                        SSC_list.append(SSC_dict)
                        detection = True

            possdet = False

        i += 1

    return detection, SSC_list


def findSSC_AIC(stream, aic_key, aic_dkey, mlowval, monsetval, minlen, useACE=False, ACE_results=[],
        satprob_weight=1., dh_bracket=[5.,10.], dh_weight=1., estt_weight=2., verbose=False):
    '''
    DEFINITION:
        This function picks out peaks in AIC analysis data that signify a
        geomagnetic storm onset.
        Adapted 2015-02-17 from MagPyAnalysis/AnalyzeActivity/stormdetect.py

    PARAMETERS:
    Variables:
        - stream:       (DataStream) Stream of 1s magnetic data with calculated AIC variables.
        - aic_key:      (str) Key of AIC variables in stream (e.g. 'var2') calculated using aic_calc.
        - aic_dkey:     (str) Key of derivative of AIC variables in stream. (e.g. 'var3')
        - mlowval:      (float) Parameter #1: stddev * mlowval = minimum threshold of peak
                        amplitude in aic_dkey for initial detection
        - monsetval:    (float) Parameter #2: stddev * monsetval = data points at peak in
                        aic_dkey above the onsetval will be extracted
        - minlen:       (int) Parameter #3: if the resulting array extracted using mlowval
                        and monsetval is greater than minlen, it is an official storm detection
    Kwargs:
        - useACE:       (bool) If True, ACE results will be implemented in detection as a
                        further criterium. Require ACE_results != []. Default = False
        - ACE_results:  (list(dict)) Results in format returned from checkACE function. Should
                        be a list of SSCs in ACE data. Default = None
        - dh_bracket:   (list) Bracket definining probabilities of detections with dh
                        within certain ranges.
        - satprob_weight:(float) Weight applied to original CME detection prob in final prob
                        calc. Default 1.
        - dh_weight:    (float) Weight applied to dH of SSC prob in final prob calculation.
                        Default 1.
        - estt_weight:  (float) Weight applied to probability calculated from arrival time in
                        relation to estimated arrival time in final prob calculation. Default 2.
        - verbose:      (bool) If True, will print variables used in detection for
                        evaluation. Default = False

    RETURNS:
        - detection:    (bool) If True, storm was detected
        - ssc_list:     (list[dict]) List of dictionaries, one for each storm in format:
                        'ssctime': time of storm detection (datetime.datetime object)
                        'amp': size of SSC in nT (float)
                        'duration': length of SSC in s (float)
                        'probf': probability that detection is real, 50-100 % (float)
                        (Only consider those with probf > 70!)

    EXAMPLE:
        >>> magdata = read(FGE_file)
        >>> satdata_1m = read(ACE_1m_file)
        >>> satdata_5m = read(ACE_5m_file)
        >>> ACE_detection, ACE_results = checkACE(satdata_1m, satdata_5m)
        >>> AIC_key = 'var2'
        >>> AIC_dkey = 'var3'
        >>> a_aic, b_aic, minlin = 5., 4., 20
        >>> magdata = magdata.aic_calc('x',timerange=timedelta(hours=0.5),aic2key=AIC_key)
        >>> magdata = magdata.differentiate(keys=[AIC_key],put2keys=[AIC_dkey])
        >>> detection, ssc_list = findSSC_AIC(magdata, AIC_key, AIC_dkey, a_aic, b_aic, minlen,
                useACE=ACE_detection, ACE_results=ACE_results)
    '''

    trange = 5.0 # in minutes

    stream = stream._drop_nans(aic_dkey)

    maxfound = True
    detection = False
    count = 0

    aicme, aicstd = stream.mean(aic_key,percentage=10,std=True)
    me, std = stream.mean(aic_dkey,percentage=10,std=True)
    if verbose == True:
        print("AIC Means", aicme, aicstd)
        print("dAIC Means", me, std)

    lowval = std * mlowval
    onsetval = std * monsetval
    minlen = minlen

    t_ind = KEYLIST.index('time')
    x_ind = KEYLIST.index('x')

    SSC_list = []

    while maxfound:
        maxval, mtime = stream._get_max(aic_dkey,returntime=True)
        if verbose == True:
            print("Max of %s found at %s" % (maxval, mtime))

        # CRITERION #1: Maximum must exceed threshold for a storm
        # *******************************************************
        if maxval > lowval:
            count += 1
            if verbose == True:
                print("Solution ", count)

            #remaininglst = [elem for elem in stream if elem.time < mtime - trange/60.0/24.0
                #or elem.time > mtime + trange/60.0/24.0]
            remaininglst = stream.copy()
            nst = stream.trim(starttime=mtime-trange/60.0/24.0, endtime=mtime+trange/60.0/24.0,
                        newway=True)
            #plot_new(nst, ['x','var2','var3'])
            for i in range(len(remaininglst.ndarray[t_ind])-1,-1,-1):
                if (remaininglst.ndarray[t_ind][i] > mtime - trange/60.0/24.0
                        and remaininglst.ndarray[t_ind][i] < mtime + trange/60.0/24.0):
                    for j in range(0, len(KEYLIST)):
                        if len(remaininglst.ndarray[j]) > 0:
                            remaininglst.ndarray[j] = np.delete(remaininglst.ndarray[j], i, 0)
            #plot_new(remaininglst, ['x','var2','var3'])

            daicmin, tmin = nst._get_min(aic_dkey, returntime=True)
            daicmax, tmax = nst._get_max(aic_dkey, returntime=True)
            if verbose == True:
                print("AIC time min:", num2date(tmin))
                print("AIC time max:", num2date(tmax))

            if tmin > tmax:     # Wrong minimum. Fix!
                while True:
                    nst = nst.trim(endtime=(tmin-1./(24.*60.*60.)))
                    if len(nst) == 0:
                        # Program is trying to correct a max/min pair at a discontinuity. Doesn't work!
                        # TODO: How to fix this?
                        if verbose == True:
                            print("An error occurred. Beware. Exiting.")
                        maxfound = False
                        break
                    daicmin, tmin = nst._get_min(aic_dkey, returntime=True)
                    daicmax, tmax = nst._get_max(aic_dkey, returntime=True)
                    if verbose == True:
                        print("False minimum. Finding a new one...")
                        print("AIC time min:", num2date(tmin))
                        print("AIC time max:", num2date(tmax))
                    if tmax > tmin:
                        break

            if not maxfound:
                break

            idx, linemin = nst.findtime(tmin)
            #xmin = linemin.x
            xmin = nst.ndarray[x_ind][idx]
            elevatedrange = nst.extract(aic_dkey,onsetval,'>')
            #ssc_init = num2date(linemin.time).replace(tzinfo=None)
            ssc_init = num2date(nst.ndarray[t_ind][idx]).replace(tzinfo=None)

            # CRITERION #2: Elevated range must be longer than minlen
            # *******************************************************
            if len(elevatedrange.ndarray[t_ind]) > minlen:
                #d_amp = elevatedrange[-1].x - xmin
                d_amp = elevatedrange.ndarray[x_ind][-1] - xmin
                #duration = (num2date(elevatedrange[-1].time).replace(tzinfo=None)-ssc_init).seconds
                duration = (num2date(elevatedrange.ndarray[t_ind][-1]).replace(tzinfo=None)-ssc_init).seconds

                if verbose == True:
                    print("Low value:", lowval)
                    print("Onset value:", onsetval)
                    print("Length of onset array:", len(nst))
                    print("Amplitude:", d_amp)
                    print("Max and time:", maxval, num2date(tmin))
                    print("Length of elevated range:", len(elevatedrange.ndarray))
                    print("Time of SSC:", ssc_init)

                # CRITERION #3: Onset in H component must exceed 10nT
                # ***************************************************
                if d_amp > d_amp_min:

                    if d_amp >= dh_bracket[0] and d_amp < dh_bracket[1]:
                        dh_prob = 50.
                    if d_amp >= dh_bracket[1]:
                        dh_prob = 100.

                    # CRITERION #4: ACE storm must have occured 45 (+-20) min before detection
                    # ************************************************************************
                    if useACE == True and ACE_results != []:
                        for sat_ssc in ACE_results:
                            det, final_probf = _calcProbWithSat(ssc_init, sat_ssc,
                                dh_prob, dh_weight, satprob_weight, estt_weight, verbose=verbose)
                            if det == True:
                                break
                            '''
                            ssc_ACE = ACE_ssc['satssctime']
                            v_arr = ACE_ssc['vwind']
                            t_arr = ssc_ACE + timedelta(minutes=((a_varr*v_arr + b_varr)*60*24))
                            t_arr_low = t_arr - timedelta(minutes=ace_window)
                            t_arr_high = t_arr + timedelta(minutes=ace_window)
                            if verbose == True:
                                print "Arrival time low:", (t_arr_low)
                                print "Arrival time high:", (t_arr_high)
                            if (ssc_init > (t_arr_low) and
                                ssc_init < (t_arr_high)): #and
                                #detection == False):
                                if verbose == True:
                                    print "Storm onset =", num2date(elevatedrange[0].time), d_amp
                                detection = True
                                SSC_dict = {}
                                SSC_dict['ssctime'] = ssc_init
                                SSC_dict['amp'] = d_amp
                                SSC_dict['duration'] = duration
                                SSC_dict['probf'] = (probf + ACE_ssc['probf'])/2.
                                ssc_list.append(SSC_dict)
                                break
                            '''
                    elif useACE == True and ACE_results == []:
                        detection, det = False, False
                        if verbose == True:
                            print("No ACE storm. False detection!")
                            print(ssc_init, d_amp)
                    elif useACE == False:
                        detection, det = True, True
                        if verbose == True:
                            print("Storm onset =", num2date(elevatedrange[t_ind][0]), d_amp)
                        detection = True
                        final_probf = dh_prob
                        if verbose == True:
                            print("No ACE data. Not sure!")

                    if det == True:
                        SSC_dict = {}
                        SSC_dict['ssctime'] = ssc_init
                        SSC_dict['amp'] = d_amp
                        SSC_dict['duration'] = duration
                        SSC_dict['probf'] = final_probf
                        SSC_list.append(SSC_dict)
                        detection = True

            else:
                if verbose == True:
                    print("Outlier -- no storm onset")
            stream = DataStream(remaininglst,{})
        else:
            maxfound = False

    return detection, SSC_list


#--------------------------------------------------------------------
#       OPTIONAL FUNCTIONS:                                         #
#               _calcDVals()                                        #
#               _calcProbWithSat()                                  #
#--------------------------------------------------------------------

def _calcDVals(stream, key, m, n):
    '''
    DEFINITION:
        This is essentially a brute force derivative method used in ACE analysis (checkACE()).
        (it deals with the noise using a sliding window average to smooth.)

    PARAMETERS:
    Variables:
        - stream:       (DataStream) Stream of satellite data
        - key:          (str) Key in stream to analyse delta-values for.
        - m:            (int) Steps in stream array to use below each data point. The
                        points within this range will be averaged and compared to the
                        n points above each data point to calculate the difference.
                        There is no extrapolation beyond boundaries so the edges will
                        not be accurate.
        - n:            (int) Steps in stream array to use above each data point for
                        comparison to m.

    RETURNS:
        - dstream:      (DataStream object) Stream containing results in the following
                        format:
                time:   Same time values as given in input stream
                x:      Unaltered data from original input key
                dx:     d_means for points m points below and n points above each
                        corresponding data point.
                var1:   means for points m points below each corresponding data point.
                var2:   standard deviations for points m points below each corresponding
                        data point.
                var3:   means for points n points above each corresponding data point.
                var4:   standard deviations for points n points above each corresponding
                        data point.
    '''

    # CHECK DATA:
    # -----------
    if len(stream) == 0:
        raise Exception("Empty stream!")

    y = stream._get_column(key)
    t = stream._get_column('time')

    dm = np.zeros(len(y))
    mean_lo, mean_hi = np.zeros(len(y)), np.zeros(len(y))
    std_lo, std_hi = np.zeros(len(y)), np.zeros(len(y))

    array = [[] for key in KEYLIST]
    headers = {}

    for i in range(1,len(y)-1):
        # DEFINE ARRAYS ABOVE AND BELOW POINT:
        # ------------------------------------
        if i < m:
            s_up = n
            s_down = i
        elif i > (len(y)-n):
            s_up = len(y) - i
            s_down = m
        else:
            s_up = n
            s_down = m
        # Note: would be good to build in boundary behaviour here: mirrored, linear?

        y_lo = np.array(y[i-s_down:i], dtype=float64)
        y_lo = np.ma.masked_array(y_lo,np.isnan(y_lo))
        y_hi = np.array(y[i:i+s_up], dtype=float64)
        y_hi = np.ma.masked_array(y_hi,np.isnan(y_hi))

        # CALCULATE MEANS:
        # ----------------
        lo_mean, lo_std = np.mean(y_lo), np.std(y_lo)
        hi_mean, hi_std = np.mean(y_hi), np.std(y_hi)

        # ASSIGN VALUES WITH DIFFERENCES:
        # -------------------------------
        array[KEYLIST.index('time')].append(float(t[i]))
        array[KEYLIST.index('x')].append(float(y[i]))
        array[KEYLIST.index('dx')].append(float(hi_mean - lo_mean))
        array[KEYLIST.index('var1')].append(float(lo_mean))
        array[KEYLIST.index('var2')].append(float(hi_mean))
        array[KEYLIST.index('var3')].append(float(lo_std))
        array[KEYLIST.index('var4')].append(float(hi_std))

    headers['col-x'] = 'X'
    headers['col-dx'] = 'dX'

    for key in ['time','x','dx','var1','var2','var3','var4']:
        array[KEYLIST.index(key)] = np.asarray(array[KEYLIST.index(key)])

    return DataStream([LineStruct()], headers, np.asarray(array))


def _calcProbWithSat(ssctime, sat_dict, dh_prob, dh_weight, satprob_weight, estt_weight, verbose=False):
    '''
    DEFINITION:
        This function calculates the probability factor (probf) of a detected storm onset
        combined with a prior ACE detection.

    PARAMETERS:
    Variables:
        - ssctime:      (datetime.datetime) Time of detection in magnetic data
        - sat_dict:     (dict) Results from sat evaluation (checkACE()) in dictionary form
        - dh_prob:      (float) Probability assigned due to variable dH
        - dh_weight:    (float) Weight assigned to dH_prob
        - satprob_weight:(float) Weight assigned to sat_prob (from sat_dict)
        - estt_weight:  (float) Weight assigned to estt_prob (calculated internally)
    Kwargs:
        - verbose:      (bool) Will print details if True.

    RETURNS:
        - If detection is real, results are:
                        (bool, float) True, final probability
        - If no real detection, results are:
                        (bool, float) False, None
    '''

    ace_window = 30
    estssctime = sat_dict['estssctime']
    t_arr_low = estssctime - timedelta(minutes=ace_window)
    t_arr_high = estssctime + timedelta(minutes=ace_window)

    if verbose == True:
        print("Arrival time low:", (t_arr_low))
        print("Arrival time high:", (t_arr_high))
    if (ssctime > (t_arr_low) and
        ssctime < (t_arr_high)):
        if verbose == True:
            print("ACE storm present. Real storm!")

        if estssctime >= ssctime:
            diff = (estssctime - ssctime).seconds / 60.
        else:
            diff = (ssctime - estssctime).seconds / 60.

        # Find probability in normal distribution:
        mu = 0.
        sigma = 10.
        base = 50.
        factor = (100.-base)/mlab.normpdf(0, mu, sigma)
        estt_prob = base + mlab.normpdf(diff, mu, sigma) * factor
        #if diff <= 10.: # minutes
        #    estt_prob = 100.
        #elif 10. < diff <= 20.:
        #    estt_prob = 75.
        #elif 20. < diff <= 30.:
        #    estt_prob = 50.
        estt_prob = estt_prob * estt_weight
        sat_prob = sat_dict['probf'] * satprob_weight
        total_weight = satprob_weight + dh_weight + estt_weight
        final_probf = (sat_prob + dh_prob + estt_prob)/total_weight

        #SSC_dict = {}
        #SSC_dict['ssctime'] = ssctime
        #SSC_dict['amp'] = d_amp
        #SSC_dict['duration'] = duration
        #SSC_dict['probf'] = final_probf
        return True, final_probf
    else:
        return False, None



if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: STORM DETECTOR PACKAGE")
    print("THIS IS A TEST RUN OF THE MAGPY OPT.STORMDET PACKAGE.")
    print("All main methods will be tested.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()

    print("Please enter path of a (variometer) data file for testing:")
    print("!! IMPORTANT: pick a day with a known SSC !!")
    print("(Examples: 2013-10-02, 2014-02-15, 2014-06-07, 2015-03-17...)")
    print("(e.g. /srv/archive/WIC/LEMI025/LEMI025_2014-05-07.bin)")
    while True:
        magdata_path = raw_input("> ")
        if os.path.exists(magdata_path):
            break
        else:
            print("Sorry, that file doesn't exist. Try again.")
            print("")

    print("")
    print("Please enter path of ACE SWEPAM data for the same day:")
    while True:
        satdata_1m_path = raw_input("> ")
        if os.path.exists(satdata_1m_path):
            break
        else:
            print("Sorry, that file doesn't exist. Try again.")
            print("")

    print("")
    print("Please enter path of ACE EPAM data for the same day:")
    while True:
        satdata_5m_path = raw_input("> ")
        if os.path.exists(satdata_5m_path):
            break
        else:
            print("Sorry, that file doesn't exist. Try again.")
            print("")

    now = datetime.utcnow()
    testrun = 'stormdettest_'+datetime.strftime(now,'%Y%m%d-%H%M')
    t_start_test = time.time()
    errors = {}
    print()
    print(datetime.utcnow(), "- Starting stormdet package test. This run: %s." % testrun)

    while True:

        # Step 1 - Read magnetic data
        try:
            magdata = read(magdata_path)
            print(datetime.utcnow(), "- Magnetic data successfully read.")
        except Exception as excep:
            errors['readmag'] = str(excep)
            print(datetime.utcnow(), "--- ERROR reading magnetic data. Aborting test.")
            break

        # Step 2 - Read ACE SWEPAM data
        try:
            satdata_1m = read(satdata_1m_path)
            print(datetime.utcnow(), "- ACE SWEPAM data successfully read.")
        except Exception as excep:
            errors['readsat1m'] = str(excep)
            print(datetime.utcnow(), "--- ERROR reading ACE SWEPAM data. Aborting test.")
            break

        # Step 3 - Read ACE EPAM data
        try:
            satdata_5m = read(satdata_5m_path)
            print(datetime.utcnow(), "- ACE EPAM data successfully read.")
        except Exception as excep:
            errors['readsat5m'] = str(excep)
            print(datetime.utcnow(), "--- ERROR reading ACE EPAM data. Aborting test.")
            break

        # Step 4 - Prepare data
        try:
            magdata = magdata.smooth('x',window_len=25)
            offsets = [21000.,0.,44000.]
            magdata.offset({'x': offsets[0], 'y': offsets[1], 'z': offsets[2]})
            magdata = magdata._convertstream('xyz2hdz')
            print(datetime.utcnow(), "- Data smoothed, offset and rotated in preparation.")
        except Exception as excep:
            errors['dataprep'] = str(excep)
            print(datetime.utcnow(), "--- ERROR preparing data (smooth, offset, _convertstream funcs).")

        # Step 5 - Plot Data (for lols)
        try:
            print(datetime.utcnow(), "- Plotting x, vwind and pflux. Everything look correct?")
            plotStreams([magdata, satdata_1m, satdata_5m], [['x'], ['var2'], ['var1']])
            print(datetime.utcnow(), "- Plotted data.")
        except Exception as excep:
            errors['plotting'] = str(excep)
            print(datetime.utcnow(), "--- ERROR plotting data.")

        # Step 6 - Use DWT3 method
        try:
            detection_DWT, ssc_list_DWT, sat_ssc_list = seekStorm(magdata, satdata_1m=satdata_1m,
                satdata_5m=satdata_5m, method='MODWT', returnsat=True)
            print(datetime.utcnow(), "- Maximal Overlap Discrete Wavelet Transform (MODWT) method applied to data.")
        except Exception as excep:
            detection_MODWT = False
            errors['MODWT'] = str(excep)
            print(datetime.utcnow(), "--- ERROR using MODWT method.")

        # Step 6 - Use AIC method
        try:
            detection_AIC, ssc_list_AIC = seekStorm(magdata, satdata_1m=satdata_1m, satdata_5m=satdata_5m,
                method='AIC')
            print(datetime.utcnow(), "- Akaike Information Criterion (AIC) method applied to data.")
        except Exception as excep:
            detection_AIC = False
            errors['AIC'] = str(excep)
            print(datetime.utcnow(), "--- ERROR using AIC method.")

        # Step 6 - Use FDM method
        try:
            detection_FDM, ssc_list_FDM = seekStorm(magdata, satdata_1m=satdata_1m, satdata_5m=satdata_5m,
                method='FDM')
            print(datetime.utcnow(), "- First Derivative method (FDM) applied to data.")
        except Exception as excep:
            detection_FDM = False
            errors['FDM'] = str(excep)
            print(datetime.utcnow(), "--- ERROR using FD method.")

        # If end of routine is reached... break.
        break

    t_end_test = time.time()
    time_taken = t_end_test - t_start_test
    print(datetime.utcnow(), "- Stream testing completed in %s s. Results below." % time_taken)

    print()
    print("----------------------------------------------------------")
    print("RESULTS:")
    print("--------")
    print()
    print("ACE:")
    i = 1
    if detection_MODWT:
        for item in sat_ssc_list:
            print("Detection # %s \n SATSSCTIME: %s \n ESTSSCTIME: %s \n VWIND: %.2f \n PFLUX: %.2f \n PROBF: %.0f" % (i,
                item['satssctime'], item['estssctime'], item['vwind'], item['pflux'], item['probf']))
            i += 1
    else:
        print("No ACE detections :(")
    print()
    print("MODWT:")
    i = 1
    if detection_DWT:
        for item in ssc_list_DWT:
            print("Detection # %s \n SSCTIME: %s \n AMP: %.2f \n DURATION %.2f \n PROBF %.0f" % (i,
                item['ssctime'], item['amp'], item['duration'], item['probf']))
            i += 1
    else:
        print("No results from this method :(")
    print()
    print("AIC:")
    i = 1
    if detection_AIC:
        for item in ssc_list_AIC:
            print("Detection # %s \n SSCTIME: %s \n AMP: %.2f \n DURATION %.2f \n PROBF %.0f" % (i,
                item['ssctime'], item['amp'], item['duration'], item['probf']))
            i += 1
    else:
        print("No results from this method :(")
    print()
    print("FDM:")
    i = 1
    if detection_FDM:
        for item in ssc_list_FDM:
            print("Detection # %s \n SSCTIME: %s \n AMP: %.2f \n DURATION %.2f \n PROBF %.0f" % (i,
                item['ssctime'], item['amp'], item['duration'], item['probf']))
            i += 1
    else:
        print(" No results from this method :(")

    print()
    print("----------------------------------------------------------")
    print("ERRORS:")
    print("-------")
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(str(errors.keys()))
        print()
        print("Would you like to print the exceptions thrown?")
        excep_answer = raw_input("(Y/n) > ")
        if excep_answer.lower() == 'y':
            i = 0
            for item in errors:
                print(errors.keys()[i] + " error string:")
                print("    " + errors[errors.keys()[i]])
                i += 1

    print()
    print("Good-bye!")
    print("----------------------------------------------------------")
