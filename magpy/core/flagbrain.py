
import sys
sys.path.insert(1,'/home/leon/Software/magpy/')
from magpy.stream import *
from magpy.core import flagging
from magpy.core.methods import get_chunks
#import csv
#import pandas as pd
import scipy
from scipy.stats import norm
from scipy.stats import entropy as scentropy
from collections import Counter
#import dateutil.parser as dparser
import matplotlib.pyplot as plt
import emd

magnetic_flagidentifiers = {0 : {'flagid' : '000', 'description' : 'normal', 'flag' : 0, 'probability' : 100},
                            1 : {'flagid' : '001', 'description' : 'lightning strike', 'flag' : 1, 'probability' : 0},
                            2 : {'flagid' : '002', 'description' : 'spike', 'flag' : 1, 'probability' : 0},
                            3 : {'flagid' : '012', 'description' : 'pulsation pc 2', 'flag' : 2, 'probability' : 0},
                            4 : {'flagid' : '013', 'description' : 'pulsation pc 3', 'flag' : 2, 'probability' : 0},
                            5 : {'flagid' : '014', 'description' : 'pulsation pc 4', 'flag' : 2, 'probability' : 0},
                            6 : {'flagid' : '015', 'description' : 'pulsation pc 5', 'flag' : 2, 'probability' : 0},
                            7 : {'flagid' : '016', 'description' : 'pulsation pi 2', 'flag' : 2, 'probability' : 0},
                            8 : {'flagid' : '020', 'description' : 'ssc geomagnetic storm', 'flag' : 2, 'probability' : 0},
                            9 : {'flagid' : '021', 'description' : 'geomagnetic storm', 'flag' : 2, 'probability' : 0},
                            10 : {'flagid' : '022', 'description' : 'crochete', 'flag' : 2, 'probability' : 0},
                            11 : {'flagid' : '030', 'description' : 'earthquake', 'flag' : 1, 'probability' : 0},
                            12 : {'flagid' : '050', 'description' : 'vehicle passing above', 'flag' : 1, 'probability' : 0},
                            13 : {'flagid' : '051', 'description' : 'nearby disturbing source', 'flag' : 1, 'probability' : 0},
                            14 : {'flagid' : '052', 'description' : 'train', 'flag' : 1, 'probability' : 0},
                            15 : {'flagid' : '090', 'description' : 'unknown disturbance', 'flag' : 1, 'probability' : 0}
                            }


def calculate_iqr(amp_curve ,f = 1.5):
    Q1 = np.nanquantile(amp_curve ,0.25)
    Q3 = np.nanquantile(amp_curve ,0.75)
    IQR = Q3 -Q1
    # define an upper limit, for which amplitudes exceeding this limit are definitly indicating disturbed data
    ul = Q3 + f *IQR
    return ul


def calculate_freq(frequ_curve ,sample_rate=1):
    mu, std = norm.fit(frequ_curve)
    x = np.linspace(0, 0.0001)
    p = norm.pdf(x, mu, std)
    # print ("Peakperiod: {} second".format(1/mu/sample_rate))
    return mu, std


def join_lst(coll):
    for i in range(len(coll ) -1 ,-1 ,-1):
        for j in range( i -1 ,-1 ,-1):
            if set(coll[j]) & set(coll[i]):
                coll[j].extend(coll.pop(i))
                break
    return [sorted(list(set(el))) for el in coll]


def define_flag_windows(n_imf, ul, ampimf, mu, std, f=5, sample_rate=1, size_factor=0.5, ignore_first_value=True,
                        debug=False):
    ndisturbed_regions = []
    win_stats = []
    amplitude_threshold = 0.2  # nT

    # gap analysis - get good and bad indicies - create bad windows array
    exceedinginds = np.argwhere(ampimf > ul)
    disturbed_seconds = len(exceedinginds)
    peakperiod_sec = 1 / mu / sample_rate
    if debug:
        print("Disturbed seconds for period:", disturbed_seconds)
    disturbed_regions = []
    win = []
    for i, el in enumerate(exceedinginds):
        el = list(el)
        if i > 0:
            diff = el[0] - exceedinginds[i - 1][0]
            if diff == 1:
                win.extend(el)
            else:
                disturbed_regions.append(win)
                win = el
        else:
            win = el
    # add last window
    if len(win) > 0 and not win in disturbed_regions:
        disturbed_regions.append(win)
    if debug:
        print("  Amount of disturbed regions:", len(disturbed_regions))

    remaining_disturbed_regions = []
    if ignore_first_value and len(disturbed_regions) > 0:
        # Drop any disturbed region containing starting index
        for el in disturbed_regions:
            if not min(el) <= 1:
                remaining_disturbed_regions.append(el)
        disturbed_regions = []
    disturbed_regions = remaining_disturbed_regions

    # for each disturbed region check whether its mean amplitude is above a given threshold - NOT USED
    remaining_disturbed_regions = []
    if len(disturbed_regions) > 0:
        for el in disturbed_regions:
            meanamp = np.mean(ampimf[el])
            if meanamp > amplitude_threshold:
                remaining_disturbed_regions.append(el)
    # disturbed_regions = remaining_disturbed_regions

    # for each disturbed region now test whether lit is special within its vicinity - NOT USED
    remaining_disturbed_regions = []
    if len(disturbed_regions) > 0:
        for el in disturbed_regions:
            start = el[0] - int(1 / mu * 10)  # 10 is arbitrary
            end = el[-1] + int(1 / mu * 10)
            if start < 0:
                start = 0
            if end > len(ampimf):
                end = len(ampimf)
            vicinityrange = ampimf[start:end]
            ulv = calculate_iqr(vicinityrange, f=3)
            periodexceedinginds = np.argwhere(vicinityrange > ulv)
            if len(periodexceedinginds) > 0:
                remaining_disturbed_regions.append(el)
    # if debug:
    #    print ("  Amount of remaining disturbed regions after checking vicinity:", len(remaining_disturbed_regions))
    # disturbed_regions = remaining_disturbed_regions

    if len(disturbed_regions) > 0:
        # Extend the size of each region by periods width before and after to be sure that beginning and end of disturbance is grapped
        extender = int(peakperiod_sec * size_factor)
        for el in disturbed_regions:
            start = el[0] - extender
            end = el[-1] + extender
            if start < 0:
                start = 0
            if end > len(ampimf) - 1:
                end = len(ampimf) - 1
            win = list(range(start, end + 1))
            win_stats.append(len(win))
            ndisturbed_regions.append(win)
        if debug:
            # List the average, maximal and mininmal lengths of disturbed ranges
            print("Window minimal length (seconds)", min(win_stats) / 1)
            print("Window maximal length (seconds)", max(win_stats) / 1)
            print("Window average length (seconds)", mean(win_stats) / 1)
    # join overlapping windows
    disturbed_regions = join_lst(ndisturbed_regions)
    if debug:
        print("  Amount of disturbed regions after combination of overlaps:", len(disturbed_regions))

    return disturbed_regions, win_stats


def window_analysis(chunk, disturbed_regions, debug=False):
    lchunk = list(chunk)
    win_len = []
    gap_len = []
    prevend = 0
    if len(disturbed_regions) > 0:
        # Extend the size of each region by periods width before and after to be sure that beginning and end of disturbance is grapped
        for dw in disturbed_regions:
            start = dw[0]
            end = dw[-1]
            if lchunk[0] <= start < lchunk[-1] or lchunk[0] <= end <= lchunk[-1]:
                # window starts or end within chunk range
                win = list(range(start, end))
                win_len.append(len(win))
                if lchunk[0] <= start < lchunk[-1] and len(win_len) > 1:
                    gap_len.append(len(list(range(prevend, start - 1))))
                prevend = end + 1
        # List the average, maximal and mininmal lengths of disturbed ranges
        if gap_len and debug:
            print("Interwindow length [seconds] and amount:", mean(gap_len) / 1, len(gap_len))
        if win_len and debug:
            print("Disturbed Window average length (seconds) and amount:", mean(win_len) / 1, len(win_len))
    return [mean(win_len), mean(gap_len), len(win_len), len(gap_len)]


def inter_windows_stats(chunks, n_imf, disturbed_regions, startindex=0, debug=False):
    # get the distribution and lengths of time windows between potentially disturbed time ranges
    # this statistic will help to identfy i.e. lightning strikes, eventually vecicles if passing and coming back
    # do this analysis time dependent run a gliding window of 2h duration across the sequence and get the
    # create 2h/2 overlapping 2h window with some characteristics on average window distances and amount
    fd = {}
    cl = []
    for chunk in chunks:
        lchunk = list(chunk)
        chunkline = [chunk[0], chunk[-1]]
        if debug:
            print(mean(chunk))
        win_char = window_analysis(chunk, disturbed_regions)
        chunkline += win_char
        cl.append(chunkline)

    for i, dw in enumerate(disturbed_regions):
        start = dw[0]
        end = dw[-1]
        for lchunk in cl:
            if lchunk[0] <= start < lchunk[1] or lchunk[0] <= end <= lchunk[1]:
                # add feature to disturbed_window_featues
                # if a disturbed window lies within or is part of a chunk
                win_char = lchunk[2:]
                cont = fd.get(i, {})
                mnew = 2
                mold = 1
                if cont.get('features', []):
                    # features have already been asigned from a previous chunk - replace if the mean of new values is larger
                    mold = np.nanmean(cont.get('features'))
                    mnew = np.nanmean(win_char)
                if mnew > mold:
                    cont['features'] = win_char
                    # if i in range(0,10):
                    #    print (win_char)
                    cont['range'] = np.array(dw) + startindex
                    cont['length'] = len(dw)
                    fd[i] = cont
        if debug:
            if i in range(0, 10):
                print(fd.get(i))
    return fd


def calculate_entropy(list_values, digits=2):
    # Ahmet Tspinar: https://ataspinar.com/2018/12/21/a-guide-for-using-the-wavelet-transform-in-machine-learning/

    def signif(x, p):
        # https://stackoverflow.com/questions/18915378/rounding-to-significant-figures-in-numpy
        x = np.asarray(x)
        x_positive = np.where(np.isfinite(x) & (x != 0), np.abs(x), 10 ** (p - 1))
        mags = 10 ** (p - 1 - np.floor(np.log10(x_positive)))
        return np.round(x * mags) / mags

    lv = signif(list_values, digits)
    counter_values = Counter(lv).most_common()
    probabilities = [elem[1] / len(list_values) for elem in counter_values]
    entropy = scentropy(probabilities)
    return [entropy]


def calculate_statistics(list_values):
    # Ahmet Tspinar: https://ataspinar.com/2018/12/21/a-guide-for-using-the-wavelet-transform-in-machine-learning/
    n5 = np.nanpercentile(list_values, 5)
    n25 = np.nanpercentile(list_values, 25)
    n75 = np.nanpercentile(list_values, 75)
    n95 = np.nanpercentile(list_values, 95)
    median = np.nanpercentile(list_values, 50)
    mean = np.nanmean(list_values)
    std = np.nanstd(list_values)
    var = np.nanvar(list_values)
    rms = np.nanmean(np.sqrt(list_values ** 2))
    # return [n5, n25, n75, n95, median, mean, std, var, rms]
    return [median, mean, std]


def calculate_crossings(list_values):
    # Ahmet Tspinar: https://ataspinar.com/2018/12/21/a-guide-for-using-the-wavelet-transform-in-machine-learning/
    # no_zero_crossings = 0
    # no_mean_crossings = 0
    # zero_crossing_indices = np.nonzero(np.diff(np.array(list_values) &gt; 0))[0]
    zero_crossing_indices = np.where(np.diff(np.sign(list_values)))[0]
    no_zero_crossings = len(zero_crossing_indices)
    # mean_crossing_indices = np.nonzero(np.diff(np.array(list_values) &gt; np.nanmean(list_values)))[0]
    mean_crossing_indices = np.where(np.diff(np.sign(list_values - np.nanmean(list_values))))[0]
    no_mean_crossings = len(mean_crossing_indices)
    return [no_zero_crossings, no_mean_crossings]


def get_fft_values(y_values, T, N, f_s):
    f_values = np.linspace(0.0, 1.0 / (2.0 * T), N // 2)
    fft_values_ = fft(y_values)
    fft_values = 2.0 / N * np.abs(fft_values_[0:N // 2])
    return f_values, fft_values


def calculate_frequency(list_values, dt=1, debug=False, diagram=False, reallydebug=False):
    # by myself
    f_values, fft_values = get_fft_values(list_values, dt, len(list_values), 1 / dt)
    # Determine approximation of peak width in fft using Gaussian approx
    FWHM = 0  # full width at half maximum
    try:
        x = f_values
        y = abs(fft_values)
        n = len(x)  # the number of data
        mean = sum(x * y) / sum(y)
        sigma = np.sqrt(sum(y * (x - mean) ** 2) / sum(y))

        def Gauss(x, a, x0, sigma):
            return a * np.exp(-(x - x0) ** 2 / (2 * sigma ** 2))

        popt, pcov = scipy.optimize.curve_fit(Gauss, x, y, p0=[max(y), mean, sigma])
        FWHM = 2 * np.sqrt(2 * np.log(2)) * popt[1]  # full width at half maximum
    except:
        pass
    # get f_value for max
    iddx = np.where(fft_values == np.max(fft_values))
    peakfrequency = f_values[iddx][0]
    peakamplitude = np.max(fft_values)
    peakwidth = FWHM
    return [peakfrequency, peakamplitude, peakwidth]


def get_features(list_values, dt=1, debug=False):
    # features for each disturbed window
    # add N surrounding disturbed regions, Gaps and average lengths within the same chunk as feature
    # add n_imf as feature

    list_len = [len(list_values)]
    entropy = [np.nan]
    crossings = [np.nan, np.nan]
    statistics = [np.nan, np.nan, np.nan]
    frequency = [np.nan, np.nan, np.nan]
    entropy = calculate_entropy(list_values)
    # if debug:
    #    print ("    ENTROPY:", entropy)
    crossings = calculate_crossings(list_values)
    # if debug:
    #    print ("    CROSSINGS:", crossings)
    #    print ("    no-zero, no-mean")
    statistics = calculate_statistics(list_values)
    # if debug:
    #    print ("    STATS:", statistics)
    #    print ("    stddev, etc")
    frequency = calculate_frequency(list_values, dt=dt, debug=debug)
    # if debug:
    #    print ("    FREQUENCY:", frequency)
    #    print ("    peakfrequency, peakamplitude,peakwidth")
    return list_len + entropy + crossings + statistics + frequency


def hh_features(IP, IF, IA, n_imf, window):
    # Determine some statistics from the HilbertHuang Transform for each "disturbed window"
    amps = IA[:, n_imf][window]
    frequs = IF[:, n_imf][window]
    phases = IP[:, n_imf][window]
    amean = np.nanmean(amps)
    astd = np.nanstd(amps)
    fmean = np.nanmean(frequs)
    fstd = np.nanstd(frequs)
    pmean = np.nanmean(phases)
    pstd = np.nanstd(phases)
    return [amean, astd, fmean, fstd, pmean, pstd]


def disturbed_region_featues(imf, IP, IF, IA, n_imf, length, disturbed_regions, startindex=0, starttime=None,
                             sample_rate=1, extfeatures=None):
    # get characteristics of disturbed regions
    # entropy, stddev etc

    # Parameter
    # wl=3600 window size and distance of overlapping windows
    # for learing do not use the disturbed regions but use chunks, get features for each chunk ?
    if not extfeatures:
        extfeatures = []
    # add basic window characteristics
    wl = 3600
    if length / 2. < wl:
        wl = int(length / 2.)
    chunks = get_chunks(length, wl=wl)
    fd = inter_windows_stats(chunks, n_imf, disturbed_regions, startindex=startindex)
    # add additional features
    for i, dw in enumerate(disturbed_regions):
        vals = imf[:, n_imf][dw]
        res = get_features(vals)  # , dt=sampling_period, debug=False)
        further_res = hh_features(IA, IF, IP, n_imf, dw)
        cont = fd.get(i)
        feat = cont.get('features')
        feat += res
        feat += further_res
        # add time-of-day and DOY, as this is important
        tod = np.nan
        doy = np.nan
        if starttime:
            mdw = int(np.mean(dw))
            centertime = starttime + timedelta(seconds=mdw * sample_rate)
            tod = int(datetime.strftime(centertime, "%H"))  # add mean of disturbed region rounded to next hour (0-23)
            doy = int(datetime.strftime(centertime, "%j"))  # add mean of disturbed region rounded to next day ( (0-366)
            feat += [tod, doy]
        if len(extfeatures):
            feat += extfeatures
        cont['features'] = feat
        fd[i] = cont
    return fd


def create_n_imf_layer(imf, minperiod=0, maxperiod=86400, sample_rate=1, startindex=0, starttime=None, factor=5,
                       determine_disturbed_region=True, extfeatures=None, debug=False):
    # perform the analysis for each imf separately
    if not extfeatures:
        extfeatures = []
    nimfdict = {}
    if not isinstance(factor, dict):
        factor = {99: 5}
    Nimf = shape(imf)[1]
    IP, IF, IA = emd.spectra.frequency_transform(imf, sample_rate, 'nht')
    for n_imf in range(0, Nimf):
        if debug:
            print("Running for IMF-{} (n_imf={})".format(n_imf + 1, n_imf))
        f = factor.get(n_imf, 5)
        nimfcont = {}
        ampimf = IA[:, n_imf]
        ul = calculate_iqr(ampimf, f=f)
        mu, std = calculate_freq(IF[:, n_imf], sample_rate=sample_rate)
        period = 1 / mu / sample_rate
        if minperiod < period <= maxperiod:
            nimfcont['IQR'] = ul
            nimfcont['f_IQR'] = f
            nimfcont['peakfrequency'] = mu
            nimfcont['peakwidth'] = std
            if debug:
                print("   - average IF: {} Hz corresponding to period {} sec".format(mu, 1 / mu / sample_rate))
            if determine_disturbed_region:
                reg, wi = define_flag_windows(n_imf, ul, ampimf, mu, std, f, size_factor=0.5)
            else:
                if debug:
                    print(" using full data signal for feature calculation")
                reg = [list(range(0, len(ampimf)))]
            nimfcont['N_disturbed_regions'] = len(reg)
            if debug:
                print("Regions looks like", reg)
            if len(reg) > 0:
                fd = disturbed_region_featues(imf, IP, IF, IA, n_imf, len(ampimf), reg, startindex=startindex,
                                              starttime=starttime, sample_rate=sample_rate, extfeatures=extfeatures)
            else:
                fd = {}
            nimfcont['disturbed_regions'] = fd
            nimfdict[n_imf] = nimfcont
            if debug:
                print(" for {}: got {} disturbed windows".format(period, len(reg)))
        else:
            if debug:
                print(" outside period range")

    return nimfdict


def magn(d, a=2.4, c=-0.43):
    # (Hauksson and Goddard, 1981, JGR)
    # Magntitude should be larger than: minmagn = func(d,a=a,c=c)
    # method is also defined in analysismethods
    return a * np.log10(d) + c


def determine_external_features(startindex=0, endindex=7200, sample_rate=1, starttime=None, externalconfig=None,
                                debug=False):
    """
    DESCRIPTION
        determine feature parameters for the selected time span from a set of predefined
        data sets
        By default the feature dictionary should include the following "independent" data if available
        - temperature variation sensor and electronics (mean + stddev) if vario
        - max xray and mean xray - goes16
        - largest earthquake in vicinity (distance, magnitude, depth)
        - lightnings (average distance, amount)
        - light switch tunnel (0 off or 1 on)
        - proton density
        - solar wind speed (mean)
    """

    endpos = 0
    if not externalconfig:
        externalconfig = [
            {"name": "temp", "path": "/home/leon/GeoSphereCloud/Daten/CobsDaten/Disturbances/storms/*.cdf",
             "components": "t1,t2,var2", "features": "mean,stddev"},
            {"name": "goes16", "path": "/home/leon/GeoSphereCloud/Daten/CobsDaten/ExternalFeatures/Goes16/*.cdf",
             "components": "x", "features": "max,mean"},
            {"name": "quake", "path": "/home/leon/GeoSphereCloud/Daten/CobsDaten/ExternalFeatures/Quakes/qakes.cdf",
             "components": "var5,f,y", "features": "value"},
            # {"name":"lightning","path":"/srv","extension":"cdf", "components":"d", "features":"mean,amount" },
            {"name": "light", "path": "/home/leon/GeoSphereCloud/Daten/CobsDaten/ExternalFeatures/Light/*.cdf",
             "components": "x", "features": "mean"},
            {"name": "ace", "path": "/home/leon/GeoSphereCloud/Daten/CobsDaten/ExternalFeatures/Ace/*.txt",
             "components": "var1,var2", "features": "mean"}
        ]
    lenfeat = [len(el.get('components').split(',')) * len(el.get('features').split(',')) for el in externalconfig]
    featurelist = [np.nan] * sum(lenfeat)
    if debug:
        print("Feature list ", featurelist)
    if not starttime:
        return featurelist
    for ext in externalconfig:
        if debug:
            print("Dealing with {}".format(ext.get('name')))
        startpos = endpos
        comps = ext.get('components').split(',')
        feats = ext.get('features').split(',')
        endpos = startpos + len(comps) * len(feats)
        endtime = starttime + timedelta(seconds=sample_rate * (endindex - startindex))
        try:
            if debug:
                print("Reading", ext.get('path'))
            extdata = read(ext.get('path'), starttime=starttime, endtime=endtime)
        except:
            extdata = DataStream()
        if extdata:
            if debug:
                print("Loaded {} datapoints".format(extdata.length()[0]))
                print("Keys", extdata.header.get('SensorKeys'))
            for comp in comps:
                col = extdata._get_column(comp)
                for feat in feats:
                    if feat in ['mean', 'average']:
                        featurelist[startpos] = np.nanmean(col)
                    elif feat in ['stddev', 'std']:
                        featurelist[startpos] = np.nanstd(col)
                    elif feat in ['max']:
                        featurelist[startpos] = np.max(col)
                    elif feat in ['amount', 'N']:
                        featurelist[startpos] = len(col)
                    elif feat in ['value']:
                        featurelist[startpos] = col
                    startpos += 1
            inds = []
            for k, el in enumerate(featurelist):
                if isinstance(el, (list, tuple, np.ndarray)):
                    if len(el) > 1:
                        if debug:
                            print("Found multiple quakes - selecting the one with heighest weighting factor")
                        # several values found - select the one with highest weight
                        # weight comes from magn function
                        # magnitude is normalized by minmagn-distance (minimum magnitude which should play a role at the given distance)
                        # values below 1 indicate that magnitudes should not play any significant role as to far away
                        if k == 8:  # hard coded index of distance value
                            w = []
                            for i, e in enumerate(el):
                                w.append(featurelist[k + 1][i] / magn(e))
                            wind = np.argmax(np.array(w))
                            featurelist[k] = el[wind]
                            featurelist[k + 1] = featurelist[k + 1][wind]
                            featurelist[k + 2] = featurelist[k + 2][wind]
                        quakeindex = k
                    else:
                        featurelist[k] = el[0]

    return featurelist


def create_feature_list(signal, max_imfs=9, disturbed_region=False, cstart=None,  sample_rate=1, externalconfig={}, debug=False):
    # Produce a full dictionary with imf information and features for each disturbed window
    featurelist = []
    comp = signal-np.mean(signal)
    imf = emd.sift.mask_sift(comp, max_imfs=max_imfs)
    extfeat=[]
    if externalconfig:
        #cstart fehlt um die external features zu rechnen
        extfeat = determine_external_features(startindex=0, endindex=len(comp), sample_rate=sample_rate, starttime=cstart,externalconfig=externalconfig)
        pass
    imfdict = create_n_imf_layer(imf,factor=f, determine_disturbed_region=disturbed_region, extfeatures=extfeat, debug=debug)
    for el in imfdict:
        #print ("Getting feature for", el) #, imfdict.get('imfcontent').get(el)).get('peakfrequency')
        feat = imfdict.get(el).get('disturbed_regions').get(0).get('features')
        feat = [0 if np.isnan(x) else x for x in feat]
        featurelist += feat
    #print (len(featurelist))
    return featurelist


def get_obs_features(obs_data, obs_labels, starttimes_obs=None, sample_rate=1, externalconfig={}):
    list_features = []
    st = None
    list_unique_labels = list(set(obs_labels))
    list_labels = [list_unique_labels.index(elem) for elem in obs_labels]
    for i,signal in enumerate(obs_data):
        if starttimes_obs:
            st = starttimes_obs[i]
        features = create_feature_list(signal, max_imfs=7, disturbed_region=False, cstart=st, sample_rate=1, externalconfig=externalconfig, debug=False)
        list_features.append(features)
    return list_features, list_labels

def get_unkown_features(data, components=['x'], externalconfig={}):
    udata_obs = []
    ulabels_obs = []
    ucstart_obs = []
    print (" Got {} data points".format(len(data)))
    sample_rate = data.samplingrate()   # obtain sampling rate
    for c in components:
        comp = data._get_column(c)
        size = get_chunks(len(comp), wl=3600)
        for s in size:
            ucstart_obs.append(data.start()+timedelta(seconds=sample_rate*s[0]))
            udata_obs.append(comp[s])
    X_unknown_obs, Y_unknown_obs = get_obs_features(udata_obs, ulabels_obs, ucstart_obs, externalconfig=externalconfig)
    return X_unknown_obs

# Create the full dictionary
def create_feature_dictionary(data, factor=5, max_imfs=15, config=None, externalconfig=None, debug=False):
    # Produce a full dictionary with imf information and features for each disturbed window
    # data needs to cover one day 86400 seconds or 1440 minutes
    if not config:
        config = {}
    if not externalconfig:
        externalconfig = {}
    analysisdict = {}
    sensorid = data.header.get('SensorID')
    comps = data.header.get('SensorKeys')
    sample_rate = data.samplingrate()
    starttime = data._find_t_limits()[0]
    print("Length of data:", data.length()[0] * sample_rate)

    if not comps:
        comps = data.header.get('DataSensorKeys')
    if not comps:
        comps = ['x', 'y', 'z']
    comps = [comp for comp in comps if comp in ['x', 'y', 'z', 'f', 'dx', 'dy', 'dz', 'df']]
    componentdict = {}
    for c in comps:
        starttimedict = {}
        if debug:
            print("Dealing with component: ", c)
        if not isinstance(factor, dict):
            f = 5
        else:
            f = factor.get(c, 5)
            if debug:
                print(" using IQR factors", f)
        comp = data._get_column(c)
        comp = comp - np.mean(comp)
        # firstly create one hour junks and analyze periods below 40 seconds
        # winlength should be half of the length you want to cover
        chunkdict = {0: {"winlength": 43200, "minperiod": 1}}
        # chunkdict = {0 : {"winlength" : 1800, "minperiod" : 1, "maxperiod" : 40},
        #             1 : {"winlength" : 10800, "minperiod" : 40, "maxperiod" : 600},
        #             2 : {"winlength" : 43200, "minperiod" : 600}}
        # chunkdict = {0 : {"winlength" : 3600, "minperiod" : 1, "maxperiod" : 40}}
        imfdict = {}
        # then create six hour junks and analyze periods below 600 seconds
        # finally analyze 1 day and check periods above 600 sec
        Nimflist = []
        nimfdict = {}
        for elem in chunkdict:
            wl = chunkdict.get(elem).get('winlength')
            minperiod = int(chunkdict.get(elem).get('minperiod', 0))
            maxperiod = int(chunkdict.get(elem).get('maxperiod', 86400))
            chunks = get_chunks(len(comp), wl=wl)
            for chunk in chunks:
                amountdict = {}
                # select imf method
                if debug:
                    print("Running for chunk", chunk)
                cstart = starttime + timedelta(seconds=sample_rate * min(chunk))
                imf = emd.sift.mask_sift(comp[list(chunk)], max_imfs=max_imfs)
                Nimf = shape(imf)[1]
                Nimflist.append(Nimf)
                # emd.plotting.plot_imfs(imf)
                # get a list of external/additional data features for each chunk and feed external features into the feature dict - do this only for dirst component
                extfeat = []
                if externalconfig:
                    extfeat = determine_external_features(startindex=min(chunk), endindex=max(chunk),
                                                          sample_rate=sample_rate, starttime=cstart,
                                                          externalconfig=externalconfig)
                nimfdict = create_n_imf_layer(imf, minperiod, maxperiod, sample_rate=sample_rate, startindex=min(chunk),
                                              starttime=cstart, factor=f, determine_disturbed_region=True,
                                              extfeatures=extfeat, debug=debug)
                nimfdict['imfs'] = Nimf
                amountdict[shape(imf)[0]] = nimfdict
                starttimedict[cstart] = amountdict
        # imf = emd.sift.ensemble_sift(comp, max_imfs=15, nensembles=24, nprocesses=6, ensemble_noise=0.001)
        # emd.plotting.plot_imfs(imf)
        # imfdict = create_n_imf_layer(imf,factor=f, disturbed_region=True, debug=debug)
        componentdict[c] = starttimedict
        # print (nimfdict)
        # imfdict['imfcontent'] = nimfdict
        # imfdict['imfs'] = max(Nimflist)
        # componentdict[c] = imfdict
    sensordict = {}
    sensordict['starttime'] = starttime
    sensordict['sampling_rate'] = sample_rate
    sensordict['amount'] = data.length()[0]
    sensordict['components'] = data.header.get('SensorKeys')
    sensordict['emddata'] = componentdict
    analysisdict[sensorid] = sensordict

    return analysisdict


def create_basic_flagdict(analysisdict, components=['x'], sensorid='Dummy', mode='magnetism', labelpredictions='',
                          suspicious=False):
    # Method to create a specific initial flagging dictionary for each component based on probability "flagrating" method
    imfflagdict = {}
    count = 0
    prob = {}
    for component in components:
        compdict = analysisdict.get(sensorid).get('emddata').get(component)
        for st in compdict:
            coverdict = compdict.get(st)
            for coverage in coverdict:
                Nimf = coverdict.get(coverage).get('imfs')
                for n_imf in range(0, Nimf):
                    # get disturbed windows fro selected imf
                    if coverdict.get(coverage).get(n_imf, False):
                        dwd = coverdict.get(coverage).get(n_imf).get('disturbed_regions')
                        disturbed_indicies = [dwd.get(el).get('range') for el in dwd]
                        disturbed_features = [dwd.get(el).get('features') for el in dwd]
                        for i, win in enumerate(disturbed_indicies):
                            winmin = min(win)
                            winmax = max(win)
                            if mode == 'ai-label':
                                # prob from flagpredictions - 001 and 002 only if n_imf <= 2
                                # data outside flagpredictions is flagged "suspicious" or not at all
                                pass
                            else:
                                prob = create_flagrating(n_imf, component, disturbed_features[i], mode=mode)
                            # winmin, winmax, flagnumber, comp, flagdescription, n_imf, prob.get('probability count')
                            imfflagdict[count] = {'start': winmin, 'end': winmax, 'flagid': prob.get('flagid'),
                                                  'flag': prob.get('flag'), 'description': prob.get('description'),
                                                  'component': component, 'n_imf': n_imf,
                                                  "probabilities": prob.get('probability count')}
                            count = count + 1
    return imfflagdict


def combine_flagid_ranges(imfflagdict, components, debug=False):
    # Combine all overlapping indices ranges with identical flagging IDs
    nimfflagdict = {}

    # extract existing flagids
    count = len(imfflagdict)
    flagidlist = list(set([imfflagdict.get(i).get('flagid') for i in range(0, count)]))
    if debug:
        print("Existing FlagID's:", flagidlist)

    # now combine all windows with similar flagid
    # asign mean probability counts if windows are joined
    wins = []
    newcount = 0
    for flagid in flagidlist:
        if debug:
            print("Combining flagid:", flagid)
        dw = []
        problist = []
        desc = ""
        flag = ""
        comps = []
        for i in range(0, count):
            if imfflagdict.get(i).get('flagid') == flagid:
                desc = imfflagdict.get(i).get('description')
                flag = imfflagdict.get(i).get('flag')
                comp = components
                indexlist = list(range(imfflagdict.get(i).get('start'), imfflagdict.get(i).get('end')))
                dw.append(indexlist)
                for ind in indexlist:
                    newl = [ind, imfflagdict.get(i).get('n_imf'), imfflagdict.get(i).get('probabilities')]
                    problist.append(newl)
        if debug:
            print("Before:", len(dw))
        jdw = join_lst(dw)
        if debug:
            print("After overlapping combination of identical FlagIDs:", len(jdw))
        for jw in jdw:
            meanproblist = []
            meannimflist = []
            for el in problist:
                if el[0] in range(jw[0], jw[-1]):
                    meanproblist.append(np.array(el[2]))
                    meannimflist.append(el[1])
            nimfflagdict[newcount] = {'start': jw[0], 'end': jw[-1], 'flagid': flagid, 'flag': flag,
                                      'description': desc, 'component': components, 'n_imf': np.nanmean(meannimflist),
                                      "probabilities": np.nanmean(meanproblist, axis=0)}
            newcount = newcount + 1
    return nimfflagdict


def get_flag_in_range(imfflagdict, startind, endind):
    for line in imfflagdict:
        l = imfflagdict.get(line)
        if startind < l.get('start') < endind and startind < l.get('end') < endind:
            print(l)


# check for overlapping windows with different identifications
# - combine if related to similar frequencies/n_imfs -> sum probability counts and obtain maximum again
# - keep separate if d(n_imf) > 2

def combine_frequency_ranges(imfflagdict, components, Nimf=15, debug=False):
    # Combine all overlapping indices ranges with identical flagging IDs
    nimfflagdict = {}

    # extract existing flagids
    count = len(imfflagdict)

    imfranges = [[n, n + 2] for n in range(0, Nimf - 1, 2)]
    if debug:
        print("Using the following imfranges:", imfranges)

    # imfranges = [[4,6]]
    # now combine all windows with similar flagid
    # asign mean probability counts if windows are joined
    wins = []
    newcount = 0
    for imfrange in imfranges:
        if debug:
            print("Combining imfranges:", imfrange)
        dw = []
        problist = []
        desc = ""
        flag = ""
        comps = []
        for i in range(0, count):
            if imfrange[0] <= imfflagdict.get(i).get('n_imf') < imfrange[1]:
                indexlist = list(range(imfflagdict.get(i).get('start'), imfflagdict.get(i).get('end')))
                dw.append(indexlist)
                for ind in indexlist:
                    newl = [ind, imfflagdict.get(i).get('n_imf'), imfflagdict.get(i).get('probabilities')]
                    problist.append(newl)
        if debug:
            print("Before:", len(dw))
        jdw = join_lst(dw)
        if debug:
            print("After overlapping combination of similar frequencies:", len(jdw))
        for jw in jdw:
            meanproblist = []
            meannimflist = []
            for el in problist:
                if el[0] in range(jw[0], jw[-1]):
                    meanproblist.append(np.array(el[2]))
                    meannimflist.append(el[1])
            # now calculate the new meanprobalilty and then asign a new flagid and description
            mprob = np.nanmean(meanproblist, axis=0)
            mnimf = np.nanmean(meannimflist)
            # print (mprob)
            most_likely_first = np.argmax(np.array(mprob))
            prob = magnetic_flagidentifiers.get(most_likely_first)
            nimfflagdict[newcount] = {'start': jw[0], 'end': jw[-1], 'flagid': prob.get('flagid'),
                                      'flag': prob.get('flag'), 'description': prob.get('description'),
                                      'component': components, 'n_imf': np.nanmean(meannimflist),
                                      "probabilities": np.nanmean(meanproblist, axis=0)}
            newcount = newcount + 1
    return nimfflagdict


def plot_flag_patches(data, components=['x'], imfflagdict={}, title=None, startind=0, endind=86400):
    # Basic plotting routine to visualize flagging patches on data
    import matplotlib.patches as patches

    edgecolor = [0.8, 0.8, 0.8]
    xinds = list(range(startind, endind, 1))
    sensorid = data.header.get('SensorID')
    heigth = int(4 * len(components))
    plt.figure(figsize=(10, heigth))
    if title:
        plt.title(title)
    for i, component in enumerate(components):
        comp = data._get_column(component)
        subplot = int("{}1{}".format(len(components), i + 1))
        plt.subplot(subplot)
        plt.plot(xinds, comp[xinds], color=[0.8, 0.8, 0.8])
        mincomp = np.nanmin(comp[xinds])
        maxcomp = np.nanmax(comp[xinds])
        for line in imfflagdict:
            l = imfflagdict.get(line)
            # print (l)
            if component in l.get('component'):
                winmin = l.get('start')
                winmax = l.get('end')
                if l.get('flag') == 1:
                    edgecolor = 'r'
                elif l.get('flag') == 2:
                    edgecolor = 'g'
                else:
                    edgecolor = 'y'
                rect = patches.Rectangle((int(winmin), mincomp), int(winmax - winmin), maxcomp - mincomp,
                                         edgecolor=edgecolor, facecolor=edgecolor, alpha=0.2)
                plt.gca().add_patch(rect)
        plt.xlim(xinds[0], xinds[-1])
        plt.xlabel('Time (samples)')
        plt.ylabel('{} [nT]'.format(component))
    # save
    # newflaglist = flaglist.add("LEMI036_1_0002", ["y","z"], 3, "Test2", "2022-08-15T23:30:30", "2022-08-15T23:45:00")


# Save flagging information to a file (dictionary)
def convert_imfflagdict_to_flaglist(sensorid, imfflagdict, starttime=None, sample_rate=1, stationid=None, groups=None):
    flaglist = flagging.Flags()
    for line in imfflagdict:
        l = imfflagdict.get(line)
        winmin = l.get('start')
        winmax = l.get('end')
        if starttime:
            # convert winmin and winmax to datetime
            winmin = starttime + timedelta(seconds=winmin * sample_rate)
            winmax = starttime + timedelta(seconds=winmax * sample_rate)
        flaglist.add(sensorid=sensorid, starttime=winmin, endtime=winmax,
                     components=l.get('component'), flagtype=l.get('flag'), labelid=l.get('flagid'),
                     comment=l.get('description'), probabilities=list(l.get('probabilities')),
                     groups=groups, stationid=stationid, operator='MagPy',
                     flagversion='2.0')
    return flaglist


# asign flags to chunks
# create a simple list of labels for each chunk
def keys_with_aximum_value(dictionary):
    max_value = max(dictionary.values())
    max_keys = [key for key, value in dictionary.items() if value == max_value]
    return max_keys


def get_flagid_for_chunk(chunk, imfflagdic, component='all'):
    idlist = []
    for d in imfflagdic:
        swin = imfflagdic.get(d).get('start')
        ewin = imfflagdic.get(d).get('end')
        comps = imfflagdic.get(d).get('component')
        if component == 'all':
            component = comps[0]
        if (swin <= min(chunk) and ewin >= max(chunk)) or min(chunk) <= swin <= max(chunk) or min(chunk) <= ewin <= max(
                chunk) and component in comps:
            idlist.append(imfflagdic.get(d).get('flagid'))
    if len(idlist) > 0:
        md = {i: idlist.count(i) for i in idlist}
        mk = keys_with_aximum_value(md)
        chunklabel = sorted(mk)[0]
    else:
        chunklabel = '000'
    return chunklabel


def extract_flagids(imfflagdict, flagids=['001']):
    # remove all flagids which are not in the provided list
    nimfflagdict = {}
    count = 0
    for line in imfflagdict:
        nl = {}
        l = imfflagdict.get(line)
        label = l.get('flagid')
        if label in flagids:
            nl['component'] = l.get('component')
            nl['flagid'] = l.get('flagid')
            nl['start'] = l.get('start')
            nl['end'] = l.get('end')
            nl['flag'] = l.get('flag')
            nl['description'] = l.get('description')
            nimfflagdict[count] = nl
            count += 1

    return nimfflagdict


@deprecated("replaced by convert_flags_to_imfflagdict")
def convert_flaglist_to_imfflagdict(name, flaglist, starttime=None, sample_rate=1):
    nfl = flaglist.flagdict.get(name)
    mdic = {}
    if not nfl:
        return mdic
    if not len(nfl) > 0:
        return mdic
    lwc = [l[:2] + l[3:] for l in nfl]
    res = []
    [res.append(x) for x in lwc if x not in res]
    nl = []
    for ind, el in enumerate(res):
        comps = []
        dic = {}
        for line in nfl:
            l = line[:2] + line[3:]
            if el == l:
                comps.append(line[2])
        # nl.append(el[:2]+ [comps] + el[2:])
        dic['component'] = comps
        if starttime:
            # convert winmin and winmax to datetime
            winmin = int(np.round((el[0] - starttime).total_seconds() / sample_rate))
            winmax = int(np.round((el[1] - starttime).total_seconds() / sample_rate))
            # print ((el[0]-starttime).total_seconds()/sample_rate)
            # print ((el[1]-starttime).total_seconds()/sample_rate)
        else:
            break
        dic['start'] = winmin
        dic['end'] = winmax
        dic['flag'] = el[2]
        desclist = [magnetic_flagidentifiers.get(d).get('description') for d in magnetic_flagidentifiers if
                    el[3].find(magnetic_flagidentifiers.get(d).get('flagid')) > -1]
        flagidlist = [magnetic_flagidentifiers.get(d).get('flagid') for d in magnetic_flagidentifiers if
                      el[3].find(magnetic_flagidentifiers.get(d).get('flagid')) > -1]
        desc = el[3]
        flid = '090'
        if len(desclist) > 0:
            desc = desclist[0]
        if len(flagidlist) > 0:
            flid = flagidlist[0]
        dic['flagid'] = flid
        dic['description'] = desc
        if not dic.get('description'):
            dic['description'] = 'not in flagid list'
        mdic[ind] = dic
    return mdic


def convert_flags_to_imfflagdict(name, flags, starttime=None, sample_rate=1):
    """
    DESCRIPTION
        converts a flagging dictionary structure into the imf dictionary version
        Both structures are actually similar except for time.
        Please note the difference to the old "convert_flaglist_.." which is based
        based on MaPy1.1.8 flagging class and its list element
    """
    nfl = flags.select('sensorid', [name])

    mdic = {}
    if not nfl:
        return mdic
    if not len(nfl) > 0:
        return mdic

    fulldic = nfl.flagdict
    for ind,subdic in enumerate(fulldic):
        el = fulldic[subdic]
        #{'sensorid': 'LEMI036_1_0002', 'starttime': datetime.datetime(2020, 5, 23, 17, 30, 58, 999988),
        #'endtime': datetime.datetime(2020, 5, 23, 17, 30, 59), 'components': ['x', 'y', 'z'], 'flagtype': 1,
        #'labelid': '001', 'label': 'lightning strike', 'comment': 'lightning RL', 'groups': None,
        #'probabilities': None, 'stationid': '', 'validity': '', 'operator': 'rename_nearby', 'color': '',
        #'modificationtime': datetime.datetime(2021, 3, 3, 10, 56, 25, 800338), 'flagversion': '2.0'}
        dic = {}
        dic['component'] = el.get("components")
        if starttime:
            # convert winmin and winmax to datetime
            winmin = int(np.round((el.get("starttime") - starttime).total_seconds() / sample_rate))
            winmax = int(np.round((el.get("endtime") - starttime).total_seconds() / sample_rate))
            # print ((el[0]-starttime).total_seconds()/sample_rate)
            # print ((el[1]-starttime).total_seconds()/sample_rate)
        else:
            break
        dic['start'] = winmin
        dic['end'] = winmax
        dic['flag'] = el.get("flagtype")
        dic['flagid'] = el.get("labelid")
        dic['description'] = el.get("label")
        mdic[ind] = dic
    return mdic


def create_flagrating(n_imf, component, features, mode='magnetism'):
    # go through all disturbed regions and add some probability counter which increases depening on the likelyhood to which features
    # are pointing to
    # i.e. if n_imf is within 0,1 and then spike probability if related to 1/window amount and smallness of window , ligthning probaility is related to window amount
    #
    # flagidentifiers are used to asign probability values 0-100 for each signature, flagid (1: remove by automatic decision, 2: keep by automatic decision)
    magnetic_flagidentifiers = {0: {'flagid': '000', 'description': 'normal', 'flag': 0, 'probability': 100},
                                1: {'flagid': '001', 'description': 'lightning strike', 'flag': 1, 'probability': 0},
                                2: {'flagid': '002', 'description': 'spike', 'flag': 1, 'probability': 0},
                                3: {'flagid': '012', 'description': 'pulsation pc 2', 'flag': 2, 'probability': 0},
                                4: {'flagid': '013', 'description': 'pulsation pc 3', 'flag': 2, 'probability': 0},
                                5: {'flagid': '014', 'description': 'pulsation pc 4', 'flag': 2, 'probability': 0},
                                6: {'flagid': '015', 'description': 'pulsation pc 5', 'flag': 2, 'probability': 0},
                                7: {'flagid': '016', 'description': 'pulsation pi 2', 'flag': 2, 'probability': 0},
                                8: {'flagid': '020', 'description': 'ssc geomagnetic storm', 'flag': 2,
                                    'probability': 0},
                                9: {'flagid': '021', 'description': 'geomagnetic storm', 'flag': 2, 'probability': 0},
                                10: {'flagid': '022', 'description': 'crochete', 'flag': 2, 'probability': 0},
                                11: {'flagid': '030', 'description': 'earthquake', 'flag': 1, 'probability': 0},
                                12: {'flagid': '050', 'description': 'vehicle passing above', 'flag': 1,
                                     'probability': 0},
                                13: {'flagid': '051', 'description': 'nearby disturbing source', 'flag': 1,
                                     'probability': 0},
                                14: {'flagid': '052', 'description': 'train', 'flag': 1, 'probability': 0},
                                15: {'flagid': '090', 'description': 'unknown disturbance', 'flag': 1, 'probability': 0}
                                }

    # features contain
    """

    0: average_disturbed_windowlength [seconds]                       12.67142857142857
    1: average_gap_length between disturbed windows [seconds]         16.xxx
    2: amount of disturbed windows within time chunk (2h)             210
    3: amount of gaps within time chunk (2h)                          209
    4: length of disturbed window                                     6
    5: entropy of imf data within disturbed window                    1.5607104090414068
    6: amount of zero-crossing of imf within disturbed window         3
    7: amount of zero-crossings when removing mean                    3
    8: median imf value                                               0.0005904629841294654
    9: mean imf value                                                 -0.00016502880204196982
    10: stdev of imf values                                            0.018773693415218912
    11: peakfrequency                                                  0.25                        ~ 4 seconds
    12: peakamplitude                                                  0.018132539517548334
    13: peakwidth                                                      0
    14: mean of instatenous amplitudes within disturbed window         3.3637792707533314
    15: std of instatenous amplitudes                                  2.0131474970441836
    16: mean of instatenous frequencies within disturbed window        0.19872917764908904         ~ 5 seconds
    17: std of instatenous frequencies                                 0.03873203758048415
    18: mean of instatenous phases within disturbed window             0.022228415922470005
    19: std of instatenous phases                                      0.00650185583842977
    20: time of day
    21: day of year
    """

    """
    Expected signatures:
    '001' : {'description' : 'lightning strike'},        : imf 0 or 1: > 10 disturbed windows, (imf 0: high imf 2 low)
    '002' : {'description' : 'spike'},                   : imf 0 or 1: < 10 disturbed windows, window length < 15 sec
    '012' : {'description' : 'pulsation pc 2'},          : imf 0 or 1: window length > 24 sec, < 10 disturbed windows, 5 < IF <= 10, >= 6 mean_crossings
    '013' : {'description' : 'pulsation pc 3'},          : imf 1 to 3: 10 < IF <= 40. >= 6 mean_crossings
    '014' : {'description' : 'pulsation pc 4'},          : imf 3 to 5:  40 < IF <= 150, >= 6 mean_crossings
    '015' : {'description' : 'pulsation pc 5'},          : imf 5 to 7: 150 < IF <= 600, >= 6 mean_crossings
    '016' : {'description' : 'pulsation pi 1'},          : imf 0 to 3: 0 < IF <= 40. window length > 75 sec (3x mean), irregular, zero != mean
    '017' : {'description' : 'pulsation pi 2'},          : imf 3 to 5: 40 < IF <= 150. window length > 300 sec (3x mean), irregular
    '020' : {'description' : 'ssc geomagnetic storm'},   : 
    '021' : {'description' : 'geomagnetic storm'},
    '022' : {'description' : 'crochete'},
    '030' : {'description' : 'earthquake'},
    '050' : {'description' : 'vehicle passing above'},    :  5 to 30 sec, <=3 zero crossings, <=4 disturbed windows, largest amplitudes in z comp
    '051' : {'description' : 'nearby disturbing source'},
    '052' : {'description' : 'train'},
    '090' : {'description' : 'unknown disturbance'}
    """

    """
    examples:
    n_imf = 2
    60.625, 585.1428571428571, 8, 7, 134, 4.184268898558329, 11, 11, 0.017238620790681225, 0.005394183872791269, 0.11594867060000137, 0.045454545454545456, 0.14086193346707337, 0, 3.01200955379042, 1.7546715014907244, 0.04166338880822934, 0.002882226366570982, 0.15909802848693325, 0.03650476972860313

    spike n_imf=0:
    12.5, 553.0, 2, 1, 15, 2.5232109529528914, 4, 4, -0.016495251868467342, -0.010782435419923738,
         0.12210212620502041,
         0.25,
         0.0852068308137039,
         0,
         3.303212877155905,
         1.7579525105728901,
         0.1491587967855677,
         0.06448090244917083,
         0.20310591230532682,
         0.1367193817133022
    """
    # We always start with maximum counter for unkown disturbance reason
    pc = [0] * len(magnetic_flagidentifiers)
    pc[15] = 8

    if n_imf < 3:
        # if features[5] < 2.0:
        #    pc[0] += 9
        pc[1] += 1
        pc[2] += 1
        pc[3] += 1
        pc[15] -= 1
        if features[2] > 20:  # amount of events in 2hours
            pc[1] += 6
            pc[2] -= 2
            pc[3] -= 2
            pc[15] -= 6
        elif features[2] > 10:  # amount of events in 2hours
            pc[1] += 4
            pc[2] -= 1
            pc[3] -= 1
            pc[15] -= 4
            if n_imf == 0:
                pc[1] += 4
                pc[15] -= 4
            elif n_imf == 1:
                pc[1] += 2
                pc[15] -= 2
        elif 4 < features[2] < 10:  # amount of events in 2hours
            if n_imf == 0:
                pc[1] += 2
            if features[1] < 600:  # average gap length 10 min
                pc[1] += 4
                pc[2] -= 1
                pc[3] -= 1
            else:  # average gap length 600sec
                pc[1] += 1
            pc[15] -= 4
            if n_imf == 0:
                pc[1] += 2
                pc[2] += 2
                pc[15] -= 4
            elif n_imf == 1:
                pc[1] += 1
                pc[15] -= 2
        else:
            pc[15] -= 4
            pc[2] += 4
            if n_imf == 0:
                pc[2] += 2
                pc[3] += 1
                pc[12] += 1
            else:
                pc[2] -= 1
                pc[3] += 2
                pc[12] += 2
        if features[7] >= 6:
            pc[2] -= 2
            pc[12] -= 2
            if n_imf < 2 and features[16] >= 0.1:
                pc[3] += 2
                if features[7] >= 10:
                    pc[3] += 2
        elif features[7] <= 4:
            pc[2] += 2
            pc[3] -= 2
        else:
            pc[2] -= 2
            pc[12] += 2
        if features[6] == features[7]:
            pc[3] += 2
    if n_imf < 4:
        pc[7] += 1
        pc[4] += 1
        pc[15] -= 1
        if features[7] >= 6:
            pc[15] -= 4
            if 0.025 <= features[16] < 0.1:
                pc[4] += 6
            if features[6] != features[7]:
                pc[4] -= 2
                pc[7] += 6
    if 3 <= n_imf <= 5:
        pc[5] += 1
        pc[8] += 1
        pc[15] -= 1
        if features[7] >= 6:
            pc[15] -= 4
            if 0.00667 <= features[16] < 0.025:
                if features[6] == features[7]:
                    pc[5] += 6
                else:
                    pc[8] += 6
    if 5 <= n_imf <= 7:
        pc[6] += 1
        pc[15] -= 1
        if features[7] >= 6:
            pc[15] -= 4
            if features[6] == features[7]:
                if 0.001667 <= features[16] < 0.00667:
                    pc[6] += 6
    if n_imf >= 8:
        pc[0] += 6
        pc[15] -= 6

    most_likely_first = np.argmax(np.array(pc))
    prob_flag = magnetic_flagidentifiers.get(most_likely_first)
    prob_flag['probability count'] = pc

    # print ("Probabilities:", pc)
    # print ("{}-{}".format(prob_flag.get('flagid'), prob_flag.get('description')))
    return prob_flag


if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: flagging brain PACKAGE")
    print("THIS IS A TEST RUN OF THE MAGPY.CORE FLAGBRAIN PACKAGE.")
    print("All main methods will be tested. This may take a while.")
    print("If errors are encountered they will be listed at the end.")
    print("Otherwise True will be returned")
    print("----------------------------------------------------------")
    print()

    amp_curve = np.asarray([1.,2.,3.,4.,5.,6.,6.,5.,3.,2.,1.,4.,5.,6.,7.])
    errors = {}

    fl = flagging.Flags()
    fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T23:56:12.654362",
                endtime="2022-11-22T23:59:12.654362", components=['x', 'y', 'z'], debug=False)
    fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T21:56:12.654362",
                endtime="2022-11-22T21:59:12.654362", components=['x', 'y', 'z'], debug=False)

    try:
        ul = calculate_iqr(amp_curve, f=1.5)
    except Exception as excep:
        errors['calculate_iqr'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR testing number.")
    try:
        st = datetime(2022,11,22)
        d = convert_flags_to_imfflagdict("LEMI025_X56878_0002_0001", fl, starttime=st, sample_rate=1)
        print(d)
    except Exception as excep:
        errors['convert_flags_to_imfflagdict'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with convert_flags_to_imfflagdict.")


    print("----------------------------------------------------------")
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(str(errors.keys()))
        print()
        print("Exceptions thrown:")
        for item in errors:
            print("{} : errormessage = {}".format(item, errors.get(item)))
