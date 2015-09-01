#!/usr/bin/env python
'''
Path:			magpy.opt.stormdet
Part of package:	stormdet
Type:			Library of functions for storm detection

PURPOSE:
	Script containing all programs needed for storm detection algorithm. 
	All functions called by seekStorm().
	Require PyWavelets package for DWT evaluations.
	Created by RLB on 2015-01-21.

CONTAINS:
    (MAIN...)
	seekStorm:	(Func) Mostly automated function that will seek a storm-
			like signal in a DataStream object.
    (SUPPORTING...)
        checkACE:	(Func) Uses input ACE data to seek out CME shocks arriving
			at the satellite.
        findSSC:	(Func) Uses data of the geomagnetic field to detect SSCs using
			the DWT or FDM method.
        findSSC_AIC: 	(Func) Uses data of the geomagnetic field to detect SSCs using
			the AIC method.
    (OPTIONAL/INTERNAL FUNCTIONS...)
	_calcDVals:	(Func) ... internal function used by checkACE.
	_calcProbWithSat:(Func) ... internal function for probability calculations
			used by findSSC and findSSC_AIC.

DEPENDENCIES:
        magpy.stream
	magpy.mpplot
	matplotlib
	pywt (PyWavelet)

CALLED BY:
	External analysis scripts only.
'''

import sys, os
try:
    from magpy.stream import *  
    from magpy.mpplot import *
except:
    sys.path.append('/home/rachel/Software/MagPyDev/magpy/trunk/src')
    from stream import *  
    from mpplot import *
import pywt, math

#--------------------------------------------------------------------
# VARIABLES
#--------------------------------------------------------------------

# Normal variables:
# Picked out using SD_VarOptimise on methods tested on all storms (vartests):
funcvars = {	'AIC':  [4.,	3.,	20],
		'DWT2': [0.000645, 60], # <-- OFFICIAL 100% VALUES #[0.0045, 45], #
		'DWT3': [2.4499e-05, 65], #[3.89999e-05, 60] (for 14)
		'FDM':  [0.0005, 55],
		'JDM':  [11,	1] 	}


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

def seekStorm(magdata, satdata_1m=None, satdata_5m=None, method='AIC', variables=None, returnsat=False,
	dwt_level='db4', dwt_var='D2', magkey='x', plot_vars=False, verbose=False):
    '''
    DEFINITION:
        Main function. Input data and it will return a dictionary detailing if
	it found a storm onset and, if so, at what time and what strength.

    PARAMETERS:
    Variables:
        - magdata: 	(DataStream) Stream of magnetic (1s) data.
    Kwargs:
	- evalfn:	(str) Evaluation function. There are three options:
			'AIC': Akaike Information Criterion (default)
			'DWT': Discrete Wavelet Transform
			'FDM': First Derivative Method
			'JDM': Jump Detection Method
	- magkey:	(str) Key in magnetic data to evaluate. Default is x (H-component)
	- method:	(str) Choice of 'AIC', 'DWT2', 'DWT3', 'FDM', 'JDM'. Default = 'AIC'
	- plot_vars:	(bool) If True, plots of variables will be shown during func call.
        - satdata_1m: 	(DataStream) Stream of 1m ACE swepam data.
        - satdata_5m: 	(DataStream) Stream of 5m ACE epam data.
	- variables:	(list) List of variables used in individual functions. Len = 2 (except AIC)
	- verbose:	(bool) If True, calculation steps will be printed.

    RETURNS:
        - detection: 	(bool) If True, storm was detected
	- ssc_list:	(list[dict]) List of dictionaries, one for each storm in format:
			'ssctime': time of storm detection (datetime.datetime object)
			'vwind': solar wind speed at storm detection (float)
			'pflux': proton flux at storm detection (float)

    EXAMPLE:
        >>> detection, ssc_list = seekStorm(magstream)
    '''

    if not variables:
        variables = funcvars[method]

    detection = False
    ssc_list = []

    if (satdata_1m and satdata_5m) != None:
        if verbose == True:
            print "Using ACE data!"
        useACE = True
    elif (satdata_1m or satdata_5m) != None:
        if verbose == True:
            print "Can't use ACE for seekStorm if only one data stream is present!"
        useACE = False
    else:
        if verbose == True:
            print "No ACE data available. Continuing without."
        useACE = False

    # EVALUATE ACE SATELLITE DATA
    # ---------------------------
    if useACE:
        try:
            ACE_detection, ACE_results = checkACE(satdata_1m, ACE_5m=satdata_5m, verbose=verbose)
            if verbose == True:
                print "ACE data successfully evaluated."
        except:
            if verbose == True:
                print "ACE evaluation failed. Continuing without."
            useACE = False
            ACE_detection, ACE_results = False, None
    else:
        ACE_detection, ACE_results = False, None

    if ACE_detection == True:
        for d in ACE_results:
            if verbose == True:
                print "ACE SSC detected at %s with speed %s and flux %s!" % (d['satssctime'], d['vwind'], d['pflux'])
    else:
        if verbose == True:
            print "No ACE storm."

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
    elif method == 'DWT2' or method == 'DWT3': # using D2 or D3 detail
        DWT = magdata.DWT_calc()
        if method == 'DWT2':
            var_key = 'var2'
        elif method == 'DWT3':
            var_key = 'var3'
        detection, ssc_list = findSSC(DWT, var_key, a, p, useACE=useACE, ACE_results=ACE_results, verbose=verbose)
        if plot_vars == True:
            plotStreams([magdata, DWT],[['x'],['dx','var1','var2','var3']],plottitle=day)

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
        print "%s is an invalid evaluation function!" % method

    if not returnsat:
        return detection, ssc_list
    else:
        return detection, ssc_list, ACE_results


#********************************************************************
#    SUPPORTING FUNCTIONS FOR seekStorm:			    #
#	checkACE()						    #
#	findSSC()						    #
#	findSSC_AIC()						    #
#********************************************************************

def checkACE(ACE_1m,ACE_5m=None,acevars={'1m':'var2','5m':'var1'},timestep=20,lastcompare=20,verbose=False,
	vwind_bracket=[380., 450.], dvwind_bracket=[40., 80.], stdlo_bracket=[15., 7.5], 
	stdhi_bracket=[0.5, 2.], pflux_bracket=[10000., 50000.],
	vwind_weight=1., dvwind_weight=1., stdlo_weight=1., stdhi_weight=1., pflux_weight=4.):
    '''
    DEFINITION:
        This function picks out rough timings of behaviour in ACE measurements that
	represent the start of a geomagnetic storm, i.e. discontinuities. The
	likelihood that an event detected by this method is a storm is determined
	according to a variety of factors.

    PARAMETERS:
    Variables:
        - ACE_1m: 	(DataStream) Stream of 1min ACE swepam data.
			Must contain: bulk solar wind speed
    Kwargs:
	- ACE_5m:	(DataStream) Stream of 5min ACE epam data
			Must contain: Proton flux 47-68
        - acevars: 	(dict) Variables containing relevant variables for solar wind
			('1m' = swepam)	and proton flux ('5m' = epam) in each stream.
			Default: '1m':'var2','5m':'var1'
	- timestep:	(int) Number of minutes to use as timestep when looking for
			storm onset. Default = 20 [minutes]
	- lastcompare:	(int) Number of minutes to compare timestep to. Default = 20 [mins]
	- verbose:	(bool) If True, will print variables used in detection for
			evaluation. Default = False

    RETURNS:
        - detection: 	(bool) If True, storm was detected. False, no storm detected.
	- ssc_list:	(list[dict]) List of dictionaries, one for each storm in format:
			'ssctime': time of storm detection (datetime.datetime object)
			'vwind': solar wind speed at storm detection (float)
			'pflux': proton flux at storm detection (float)

    EXAMPLE:
        >>> ACE_det, ACE_ssc_list = checkACE(ACE_1m,ACE_5m=ACE_5m,timestep=20,lastcompare=20,verbose=True)
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

        # Check for large percentages of nans:
        ar = [elem for elem in dv_test if not isnan(elem)]
        div = float(len(ar))/float(len(dv_test))*100.0
        if div < 5.:
            if verbose == True:
                print "Too many nans!"
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
                logger.info("Too many v_wind NaNs... searching for value...")
                i += 1
        except:
            v_max = v_test[i_max]

        # CALCULATE PROB THAT DETECTION IS REAL STORM:
        # --------------------------------------------
        if dv_max >= 25.:

            t_lo = num2date(t_max) - timedelta(minutes=timestep)
            t_hi = num2date(t_max) + timedelta(minutes=lastcompare)
            dACE_lo = dACE.trim(starttime=t_lo-timedelta(minutes=60), endtime=t_lo, newway=True)
            dACE_hi = dACE.trim(starttime=t_hi, endtime=t_hi+timedelta(minutes=60), newway=True)

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
                print num2date(t_max), v_max, dv_max, std_lo, std_hi
                print num2date(t_max), v_prob, dv_prob, stlo_prob, sthi_prob, '=', probf

            # Detection is likely a storm if total probability is >= 70.
            #if probf >= 70.:
            # Check ACE EPAM data for rise in solar proton flux
            if ACE_5m != None:
                t_5m = ACE_5m._get_column('time')
                t_val, idx = find_nearest(t_5m,t_max)
                start, end = (num2date(t_val), 
			num2date(t_val)+timedelta(minutes=10))
                ACE_flux = ACE_5m.trim(starttime=start,endtime=end, newway=True)
                flux_val = ACE_flux.mean(key_e,percentage=20)
                if isnan(flux_val):
                    # First try larger area:
                    ACE_flux = ACE_5m.trim(starttime=start-timedelta(minutes=10), 
			endtime=(end+timedelta(minutes=20)), newway=True)
                    flux_val = ACE_flux.mean(key_e,percentage=20)
                    if isnan(flux_val):
                        print "Proton Flux is nan!", flux_val
                        flux_val = 0.
                        #raise Exception
                if verbose == True:
                    print "--> Jump in solar wind speed detected! Proton flux at %s." % flux_val

		# FINAL REQUIREMENT:
		# ******************
		# Proton flux (47-68 keV) must exceed 10000. Often it reaches above 200000 during 
		# a storm, but quiet time values rarely exceed 10000.
                if flux_val < pflux_bracket[0]:
                    pflux_prob = 0.
                    if verbose == True:
                        print "That's not really a storm!"
                        print ace_ssc
                elif pflux_bracket[0] <= flux_val < pflux_bracket[1]:
                    pflux_prob = 50.
                    if verbose == True:
                        print "That's almost a storm!"
                        print ace_ssc
                elif flux_val >= pflux_bracket[1]:
                    pflux_prob = 100.
                    if verbose == True:
                        print "That's almost a storm!"
                        print ace_ssc
                pflux_prob = pflux_prob * pflux_weight
                probf = probf * total_weight
                prob_var = (probf + pflux_prob)/(total_weight + pflux_weight)
                flux_var = flux_val
                #else:
                #    prob_var = probf
                #    flux_Var = None
                #    if verbose == True:
                #        print "That's almost a storm!"
                #        print ace_ssc

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
                acedict['probf'] = prob_var
                detection = True
                ace_ssc.append(acedict)

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
        print "ACE storms detected at:"
        for item in ace_ssc:
            print item['satssctime']

    return detection, ace_ssc


def findSSC(var_stream, var_key, a, p, verbose=False, useACE=False, ACE_results=None,
	dh_bracket=[5.,10.], satprob_weight=1., dh_weight=1., estt_weight=2.):
    '''
    DEFINITION:
        This function works on data evaluated using either a Discrete Wavelet
	Transform (DWT) or a First Derivative (FDM). It takes a stream and will
	extract a maximum in that stream that exceeds the amplitude threshhold
	a for the period p.

    PARAMETERS:
    Variables:
        - stream: 	(DataStream) Stream containing data to be analysed (e.g. DWT stream)
	- var_key:	(str) Key of variable to be analysed in stream
	- a:		(float) Minimum amplitude threshold for peak
	- p:		(float) Minimum duration peak must exceed 
    Kwargs:
        - useACE: 	(bool) If True, ACE results will be implemented in detection as a
			further criterium. Require ACE_results != None. Default = False
	- ACE_results:	(list(dict)) Results in format returned from checkACE function. Should
			be a list of SSCs in ACE data. Default = None
	- verbose:	(bool) If True, will print variables used in detection for
			evaluation. Default = False

    RETURNS:
        - detection: 	(bool) If True, storm was detected
	- ssc_list:	(list[dict]) List of dictionaries, one for each storm in format:
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
        >>> DWT = magdata.DWT_calc()
        >>> var_key = 'var2'		# use second detail D2
	>>> a, p = 0.0004, 45		# amplitude 0.0004 in D2 var must be exceeded over period 45 seconds
        >>> detection, ssc_list = findSSC(DWT, var_key, a, p, useACE=useACE, ACE_results=ACE_results, verbose=verbose)
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
        print "Starting analysis with findSSC()."
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
                    print "x1:", x1, ssc_init
                    #print "x2:", x2, num2date(row.time)
                    print "x2:", x2, num2date(t_ar[i])
                    print "Possible detection with duration %s at %s with %s nT." % (duration, ssc_init, d_amp)

                # CRITERION #3: Variation in H must exceed a certain value
	        # ********************************************************
                if d_amp > d_amp_min:

                    if d_amp >= dh_bracket[0] and d_amp < dh_bracket[1]:
                        dh_prob = 50.
                    if d_amp >= dh_bracket[1]:
                        dh_prob = 100.

                    if verbose == True:
                        print "Detection at %s with %s nT!" % (ssc_init,str(d_amp))

                    # CRITERION #4: Storm must have been detected in ACE data
                    # *******************************************************
                    if useACE == True and ACE_results != None:

                        # CRITERION #5: ACE storm must have occured 45 (+-20) min before detection
                        # ************************************************************************
                        for sat_ssc in ACE_results:
                            det, final_probf = _calcProbWithSat(ssc_init, sat_ssc,
				dh_prob, dh_weight, satprob_weight, estt_weight, verbose=verbose)
                            if det == True:
                                break
                            '''
                            ssc_ACE = sat_ssc['satssctime']
                            v_arr = sat_ssc['vwind']
                            t_arr = ssc_ACE + timedelta(minutes=((a_varr*v_arr + b_varr)*60*24))
                            t_arr_low = t_arr - timedelta(minutes=ace_window)
                            t_arr_high = t_arr + timedelta(minutes=ace_window)
                            sat_prob = sat_ssc['probf'] * satprob_weight
                            if verbose == True:
                                print "Arrival time low:", (t_arr_low)
                                print "Arrival time high:", (t_arr_high)
                            if (ssc_init > (t_arr_low) and
				ssc_init < (t_arr_high)):
                                date = datetime.strftime(ssc_init, '%Y-%m-%d')
                                if verbose == True:
                                    print "ACE storm present. Real storm!"
                                    print ssc_init, d_amp

                                estssctime = aceresults['estssctime']
                                ssctime = mag_results['ssctime']
                                if estssctime >= ssctime:
                                    diff = (estssctime - ssctime).seconds / 60.
                                else:
                                    diff = (ssctime - estssctime).seconds / 60.
                                if diff <= 10.: # minutes
                                    estt_prob = 100.
                                elif 10. < diff <= 20.:
                                    estt_prob = 75.
                                elif 20. < diff <= 30.:
                                    estt_prob = 50.
                                estt_prob = estt_prob * estt_weight

                                total_weight = satprob_weight + dh_weight + estt_weight
                                item['probf'] = (sat_prob + dh_prob + estt_prob)/total_weight

                                SSC_dict = {}
                                SSC_dict['ssctime'] = ssc_init
                                SSC_dict['amp'] = d_amp
                                SSC_dict['duration'] = duration
                                SSC_dict['probf'] = (probf + ACE_ssc['probf'])/2.
                            '''
                    elif useACE == True and ACE_results == None:
                        detection == False
                        if verbose == True:
                            print "No ACE storm. False detection!"
                            print ssc_init, d_amp
                    elif useACE == False:
                        detection = True
                        final_probf = dh_prob/dh_weight
                        if verbose == True:
                            print "No ACE data. Not sure!"

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


def findSSC_AIC(stream, aic_key, aic_dkey, mlowval, monsetval, minlen, useACE=False, ACE_results=None,
	satprob_weight=1., dh_weight=1., estt_weight=2., verbose=False):
    '''
    DEFINITION:
        This function picks out peaks in AIC analysis data that signify a 
	geomagnetic storm onset.
        Adapted 2015-02-17 from MagPyAnalysis/AnalyzeActivity/stormdetect.py

    PARAMETERS:
    Variables:
        - stream: 	(DataStream) Stream of 1s magnetic data with calculated AIC variables.
	- aic_key:	(str) Key of AIC variables in stream (e.g. 'var2') calculated using aic_calc.
	- aic_dkey:	(str) Key of derivative of AIC variables in stream. (e.g. 'var3')
	- mlowval:	(float) Parameter #1: stddev * mlowval = minimum threshold of peak
			amplitude in aic_dkey for initial detection
	- monsetval:	(float) Parameter #2: stddev * monsetval = data points at peak in
			aic_dkey above the onsetval will be extracted
	- minlen:	(int) Parameter #3: if the resulting array extracted using mlowval
			and monsetval is greater than minlen, it is an official storm detection
    Kwargs:
        - useACE: 	(bool) If True, ACE results will be implemented in detection as a
			further criterium. Require ACE_results != None. Default = False
	- ACE_results:	(list(dict)) Results in format returned from checkACE function. Should
			be a list of SSCs in ACE data. Default = None
	- verbose:	(bool) If True, will print variables used in detection for
			evaluation. Default = False

    RETURNS:
        - detection: 	(bool) If True, storm was detected
	- ssc_list:	(list[dict]) List of dictionaries, one for each storm in format:
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
        print "AIC Means", aicme, aicstd
        print "dAIC Means", me, std

    lowval = std * mlowval
    onsetval = std * monsetval
    minlen = minlen

    t_ind = KEYLIST.index('time')
    x_ind = KEYLIST.index('x')

    SSC_list = []

    while maxfound:
        maxval, mtime = stream._get_max(aic_dkey,returntime=True)
        if verbose == True:
            print "Max of %s found at %s" % (maxval, mtime)

        # CRITERION #1: Maximum must exceed threshold for a storm
	# *******************************************************
        if maxval > lowval:
            count += 1
            if verbose == True:
                print "Solution ", count

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
                print "AIC time min:", num2date(tmin)
                print "AIC time max:", num2date(tmax)

            if tmin > tmax:	# Wrong minimum. Fix!
                while True:
                    nst = nst.trim(endtime=(tmin-1./(24.*60.*60.)))
                    if len(nst) == 0:
                        # Program is trying to correct a max/min pair at a discontinuity. Doesn't work!
                        # TODO: How to fix this?
                        if verbose == True:
                            print "An error occurred. Beware. Exiting."
                        maxfound = False
                        break
                    daicmin, tmin = nst._get_min(aic_dkey, returntime=True)
                    daicmax, tmax = nst._get_max(aic_dkey, returntime=True)
                    if verbose == True:
                        print "False minimum. Finding a new one..."
                        print "AIC time min:", num2date(tmin)
                        print "AIC time max:", num2date(tmax)
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
                    print "Low value:", lowval
                    print "Onset value:", onsetval
                    print "Length of onset array:", len(nst)
                    print "Amplitude:", d_amp
                    print "Max and time:", maxval, num2date(tmin)
                    print "Length of elevated range:", len(elevatedrange.ndarray)
                    print "Time of SSC:", ssc_init

                # CRITERION #3: Onset in H component must exceed 10nT
	        # ***************************************************
                if d_amp > d_amp_min:

                    if d_amp >= 5. and d_amp < 10.:
                        probf = 50.
                    if d_amp >= 10.:
                        probf = 100.

                    # CRITERION #4: ACE storm must have occured 45 (+-20) min before detection
                    # ************************************************************************
                    if useACE == True and ACE_results != None:
                        for sat_ssc in ACE_results:
                            det, final_probf = _calcProbWithSat(ssc_init, sat_ssc,
				probf, dh_weight, satprob_weight, estt_weight, verbose=verbose)
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
                    elif useACE == True and ACE_results == None:
                        if verbose == True:
                            print "No ACE storm. False detection!"
                            print ssc_init, d_amp
                    elif useACE == False:
                        if verbose == True:
                            #print "Storm onset =", num2date(elevatedrange[0].time), d_amp
                            print "Storm onset =", num2date(elevatedrange[t_ind][0]), d_amp
                        detection = True
                        final_probf = probf
                        if verbose == True:
                            print "No ACE data. Not sure!"

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
                    print "Outlier -- no storm onset"
            stream = DataStream(remaininglst,{})
        else:
            maxfound = False

    return detection, SSC_list


#--------------------------------------------------------------------
#    	OPTIONAL FUNCTIONS:					    #
#		_calcDVals()					    #
#		_calcProbWithSat()				    #
#--------------------------------------------------------------------

def _calcDVals(stream, key, m, n):
    '''
    DEFINITION:
        This is essentially a brute force derivative method used in ACE analysis (checkACE()).
	(it deals with the noise using a sliding window average to smooth.)

    PARAMETERS:
    Variables:
        - stream: 	(DataStream) Stream of satellite data
	- key:		(str) Key in stream to analyse delta-values for.
	- m:		(int) Steps in stream array to use below each data point. The
			points within this range will be averaged and compared to the
			n points above each data point to calculate the difference.
			There is no extrapolation beyond boundaries so the edges will
			not be accurate.
	- n:		(int) Steps in stream array to use above each data point for
			comparison to m.

    RETURNS:
        - dstream: 	(DataStream object) Stream containing results in the following
			format:
		time:	Same time values as given in input stream
		x:	Unaltered data from original input key
		dx:	d_means for points m points below and n points above each 
			corresponding data point.
		var1:	means for points m points below each corresponding data point.
		var2:	standard deviations for points m points below each corresponding 
			data point.
		var3:	means for points n points above each corresponding data point.
		var4:	standard deviations for points n points above each corresponding 
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

    dstream = DataStream([],{})

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
        row = LineStruct()
        row.time = t[i]
        row.x = y[i]
        row.dx = hi_mean - lo_mean
        row.var1 = lo_mean
        row.var2 = hi_mean
        row.var3 = lo_std
        row.var4 = hi_std

        dstream.add(row)

    return dstream


def _calcProbWithSat(ssctime, sat_dict, dh_prob, dh_weight, satprob_weight, estt_weight, verbose=False):
    '''
    DEFINITION:
        This function calculates the probability factor (probf) of a detected storm onset
	combined with a prior ACE detection.

    PARAMETERS:
    Variables:
        - ssctime: 	(datetime.datetime) Time of detection in magnetic data
	- sat_dict:	(dict) Results from sat evaluation (checkACE()) in dictionary form
	- dh_prob:	(float) Probability assigned due to variable dH
	- dh_weight:	(float) Weight assigned to dH_prob
	- satprob_weight:(float) Weight assigned to sat_prob (from sat_dict)
	- estt_weight:	(float)	Weight assigned to estt_prob (calculated internally)
    Kwargs:
	- verbose:	(bool) Will print details if True.

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
        print "Arrival time low:", (t_arr_low)
        print "Arrival time high:", (t_arr_high)
    if (ssctime > (t_arr_low) and
	ssctime < (t_arr_high)):
        if verbose == True:
            print "ACE storm present. Real storm!"

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


