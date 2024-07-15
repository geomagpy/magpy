import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import *
import copy
from scipy.stats import norm
from scipy.interpolate import UnivariateSpline

class k_fmi(object):
    """
    DESCRIPTION:
        determination of K values based on the FMI method. This class is derived from the original C code
        K_index.h by Lasse  Hakkinen, Finnish Meteorological Institute. Please note: the original method
        fills data gaps based on mean values if gaps are smaller than a given maximum gap length. To replicate
        that behavior,
        Details on the procedure can be found
        here: citation
    PARAMETERS:
        datastream  : a magpy data stream containing geomagnetic x and y
                    components. Field values need to be provided in nT.
                    The FMI method requires three full days to analyse the
                    middle day. The given datastream will be tested and
                    eventually cut into fitting pieces for the analysis.
                    Furthermore, the method will be applied on one-minute
                    data. Thus, the datastream will eventually be filtered
                    using standard IAGA recommendations if high resolution
                    data is provided.
        K9_limit    : K=9 limit for the particular observatory in nT.
                      If contained within the datastream structure
                    this data will be used automatically. Providing a manual
                    value will override the value contained in the datastream.
        longitude   : longitude of the observatory whose K indices are to be
                    computed. If contained within the datastream structure
                    this data will be used automatically. Providing a manual
                    value will override the value contained in the datastream.
                    The longitude is used to determine the time of local
                    midnight.
        missing_data : Marker for missing data point (e.g. 999999).
    RETURNS:
        K_table     : a list containing the 8 K-indicies for the given day
                    (better return a data stream with k-inidcies
    REQUIREMENTS:
       feed data for three days into the scheme, the middle day will be analyzed
    APPLICATION:
        kvals = K_index(datastream, K9_limit=500, longitude=20.0, missing_data=999999)

    """

    def __init__(self, step_size=60, K9_limit=750, longitude=222.0, missing_data=999999):

        # get variables
        self.longitude = longitude
        # initialize variables
        self.pointsinday = (24 * 3600) // step_size
        self.datacount = 3 * self.pointsinday
        self.missing_data = missing_data
        self.step_size = step_size
        self.HarmOrder = 5  # Order of harmonic fitting
        KExponent = 3.3  # Exponent in K^KExponent
        self.max_gap_length = 15 * 60  # Maximum length of gap of missing data points in seconds
        K9_limit = K9_limit * 10  # conversion to 0.1 nT

        # initialize arrays
        DefLength = [0] * 24
        self.K_length = [0] * 10
        self.FitLength = [0] * 24
        self.DefLength = [0] * 24
        self.K_limit = [0] * 9
        DefLimit = [75, 150, 300, 600, 1050, 1800, 3000, 4950, 7500]
        for i in range(9):
            self.K_limit[i] = (K9_limit * DefLimit[i]) // 7500
        for i in range(3):
            self.DefLength[i] = 90 * 60  # NightLength
        for i in range(3, 6):
            self.DefLength[i] = 60 * 60  # DawnLength
        for i in range(6, 18):
            self.DefLength[i] = 0
        for i in range(18, 21):
            self.DefLength[i] = 60 * 60  # DuskLength
        for i in range(21, 24):
            self.DefLength[i] = 90 * 60  # NightLength
        newdef = []
        for i in range(24):
            newdef.append(self.DefLength[(int(i + (longitude // 15)) % 24)])
        self.DefLength = newdef

        self.K_length[0] = 0
        for i in range(1, 10):
            self.K_length[i] = int(60 * math.exp(KExponent * math.log(i)))
            if self.K_length[i] > 18 * 3600:
                self.K_length[i] = 18 * 3600

    def fill_gaps(self, data_array):
        StepSize = self.step_size
        MaxGapLength = self.max_gap_length
        MissingPoint = self.missing_data
        i = 0
        while i < len(data_array):
            while i < len(data_array) and data_array[i] != MissingPoint:
                i += 1

            if i < len(data_array):
                gap_index = i
                while i < len(data_array) and data_array[i] == MissingPoint:
                    i += 1
                if gap_index > 0 and i < len(data_array):
                    gap_length = i - gap_index
                    if gap_length * StepSize < MaxGapLength:
                        value1 = data_array[gap_index - 1]
                        value2 = data_array[i]
                        for j in range(1, gap_length + 1):
                            data_array[gap_index] = value1 + (j * (value2 - value1)) // (gap_length + 1)
                            gap_index += 1
        return data_array

    def CopyData(self, FromArray):
        ToArray = FromArray[self.pointsinday:2 * self.pointsinday]
        return ToArray

    def ComputeDiff(self, Array1, Array2):
        DiffArray = np.zeros(self.pointsinday, dtype=int)
        Array1 = Array1[self.pointsinday:2 * self.pointsinday]
        for i in range(self.pointsinday):
            if Array1[i] != self.missing_data and Array2[i] != self.missing_data:
                DiffArray[i] = Array1[i] - Array2[i]
            else:
                DiffArray[i] = self.missing_data
        return DiffArray

    def K_MaxMin(self, Data_array, HourIndex):
        # global PointsInDay, MissingPoint, self.K_limit
        start = HourIndex * self.pointsinday // 8
        end = start + self.pointsinday // 8
        Max = Min = Data_array[start]
        for i in range(start + 1, end):
            if Data_array[i] > Max:
                Max = Data_array[i]
            elif Data_array[i] < Min:
                Min = Data_array[i]
        if Max == self.missing_data:
            return -1
        else:
            for i in range(9):
                if Max - Min <= self.K_limit[i]:
                    return i
            return 9

    def FillKTable(self, X_data, Y_data, X_harm=None, Y_harm=None, Subtract=False):
        K_table = []
        if Subtract:
            X_temp = self.ComputeDiff(X_data, X_harm)
            Y_temp = self.ComputeDiff(Y_data, Y_harm)
        else:
            X_temp = self.CopyData(X_data)
            Y_temp = self.CopyData(Y_data)

        for i in range(8):
            K_X = self.K_MaxMin(X_temp, i)
            K_Y = self.K_MaxMin(Y_temp, i)
            K_table.append(max(K_X, K_Y))
        return K_table

    def FindLengths(self, K_table):
        FitLength = [0] * 24
        for i in range(24):
            if K_table[i // 3] < 0:
                FitLength[i] = 30 * 60 + self.DefLength[i]
            else:
                FitLength[i] = 30 * 60 + self.DefLength[i] + self.K_length[K_table[i // 3]]
            if FitLength[i] > 24 * 3600:
                FitLength[i] = 24 * 3600
        return FitLength

    def ComputeMean(self, hour, Data_array, FitLength):
        MiddlePoint = self.pointsinday + (60 * (60 * hour + 30)) // self.step_size
        WingLength = FitLength[hour] // self.step_size
        Data_array = Data_array[MiddlePoint - WingLength:MiddlePoint + WingLength + 1]
        Sum = 0.0
        Count = 2 * WingLength + 1
        for i in range(Count):
            if Data_array[i] == self.missing_data:
                return self.missing_data
            else:
                Sum += Data_array[i]
        return int(Sum / Count)

    def InterpolateMean(self, Mean_array):
        hour0 = 0
        hour1 = 23

        while hour0 < 24 and Mean_array[hour0] == self.missing_data:
            hour0 += 1
        if hour0 < 24:
            for hour in range(hour0):
                Mean_array[hour] = Mean_array[hour0]

        while hour1 > hour0 and Mean_array[hour1] == self.missing_data:
            hour1 -= 1
        for hour in range(hour1 + 1, 24):
            Mean_array[hour] = Mean_array[hour1]

        while hour0 < hour1:
            found = False
            while hour0 < hour1 and not found:
                hour0 += 1
                found = (Mean_array[hour0] == self.missing_data)
            if found:
                hour = hour0
                while hour0 < hour1 and Mean_array[hour0] == self.missing_data:
                    hour0 += 1
                GapLength = hour0 - hour
                Value1 = Mean_array[hour - 1]
                Value2 = Mean_array[hour0]
                for i in range(1, GapLength + 1):
                    Mean_array[hour] = Value1 + (i * (Value2 - Value1)) // (GapLength + 1)
                    hour += 1
        return Mean_array

    def ComputeHourAverages(self, X_data, Y_data, FitLength):
        # global X_mean, Y_mean
        X_mean = [0] * 24
        Y_mean = [0] * 24
        for hour in range(24):
            X_mean[hour] = self.ComputeMean(hour, X_data, FitLength)
            Y_mean[hour] = self.ComputeMean(hour, Y_data, FitLength)

        X_mean = self.InterpolateMean(X_mean)
        Y_mean = self.InterpolateMean(Y_mean)
        return X_mean, Y_mean

    def ComputeHarmonicFit(self, X_mean, Y_mean):
        # global X_mean, Y_mean, X_harm, Y_harm, HarmOrder, PointsInDay, StepSize
        X_harm = np.zeros(self.pointsinday, dtype=int)
        Y_harm = np.zeros(self.pointsinday, dtype=int)

        # Initialize variables
        t0 = 30 * 60  # Middle point of first hour in seconds
        t1 = 23 * 3600 + t0  # Middle point of last hour in seconds

        # Rotate the curve so that start and end points are equal
        X_coeff = (X_mean[23] - X_mean[0]) / (t1 - t0)
        Y_coeff = (Y_mean[23] - Y_mean[0]) / (t1 - t0)
        t = t0
        for i in range(24):
            X_mean[i] -= int(X_coeff * (t - t0))
            Y_mean[i] -= int(Y_coeff * (t - t0))
            t += 3600

        # Compute the Fourier coefficients
        ReX = [0.0] * 13
        ImX = [0.0] * 13
        ReY = [0.0] * 13
        ImY = [0.0] * 13

        for i in range(self.HarmOrder + 1):
            ReX[i] = X_mean[0]
            ImX[i] = 0.0
            ReY[i] = Y_mean[0]
            ImY[i] = 0.0
            angle = -i * (2.0 * np.pi / 24.0)
            for k in range(1, 24):
                si = np.sin(k * angle)
                co = np.cos(k * angle)
                ReX[i] += X_mean[k] * co
                ImX[i] += X_mean[k] * si
                ReY[i] += Y_mean[k] * co
                ImY[i] += Y_mean[k] * si

        # Compute the inverse Fourier transform taking into account only terms up to HarmOrder
        Xptr = np.zeros(self.pointsinday, dtype=int)
        Yptr = np.zeros(self.pointsinday, dtype=int)
        t = 0
        angle = 2.0 * np.pi * (23.0 / 24.0) / (t1 - t0)
        for i in range(self.pointsinday):
            Xptr[i] = int(ReX[0])
            Yptr[i] = int(ReY[0])
            angle2 = (t - t0) * angle
            for k in range(1, self.HarmOrder + 1):
                si = np.sin(k * angle2)
                co = np.cos(k * angle2)
                Xptr[i] += int(2.0 * (ReX[k] * co - ImX[k] * si))
                Yptr[i] += int(2.0 * (ReY[k] * co - ImY[k] * si))
            # Rotate the curve back
            Xptr[i] = int(Xptr[i] / 24.0 + X_coeff * (t - t0))
            Yptr[i] = int(Yptr[i] / 24.0 + Y_coeff * (t - t0))
            t += self.step_size

        X_harm[:] = Xptr
        Y_harm[:] = Yptr

        return X_mean, Y_mean, X_harm, Y_harm

    def GetTimes(self, Time_array):
        # Not part of the original collection
        tr = []
        for hour3 in range(8, 16):
            MiddlePoint = self.pointsinday + (180 * (60 * hour3 + 30)) // self.step_size
            tr.append(Time_array[MiddlePoint])
        return tr


def K_fmi(datastream, step_size=60, K9_limit=750, longitude=222.0, missing_data=999999, test=False, debug=False):
    """
    DESCRIPTION:
        determination of K values based on the FMI method. This class is derived from the original C code
        K_index.h by Lasse  Hakkinen, Finnish Meteorological Institute. Please note: the original method
        fills data gaps based on mean values if gaps are smaller than a given maximum gap length. To replicate
        that behavior,
        Details on the procedure can be found
        here: citation
    PARAMETERS:
        datastream  : a magpy data stream containing geomagnetic x and y
                    components. Field values need to be provided in nT.
                    The FMI method requires three full days to analyse the
                    middle day. The given datastream will be tested and
                    eventually cut into fitting pieces for the analysis.
                    Furthermore, the method will be applied on one-minute
                    data. Thus, the datastream will eventually be filtered
                    using standard IAGA recommendations if high resolution
                    data is provided.
        K9_limit    : K=9 limit for the particular observatory in nT.
                      If contained within the datastream structure
                    this data will be used automatically. Providing a manual
                    value will override the value contained in the datastream.
        longitude   : longitude of the observatory whose K indices are to be
                    computed. If contained within the datastream structure
                    this data will be used automatically. Providing a manual
                    value will override the value contained in the datastream.
                    The longitude is used to determine the time of local
                    midnight.
        missing_data : Marker for missing data point (e.g. 999999).
    RETURNS:
        datastream  : a datastream containig the 8 K-indicies for each given day
                    asocitaed with key 'var1'
    REQUIREMENTS:
       feed data for at least three days into the scheme, only the middle day will be analyzed
    APPLICATION:
        kvals = K_fmi(datastream, K9_limit=500, longitude=20.0, missing_data=999999)
    """
    fulldataarray = [[[], [], []]]

    if datastream:
        fulldataarray, sr, k9 = datastream.get_fmi_array(missing_data=missing_data, debug=debug)  # return [],[]

    tresult = []
    kresult = []
    array = [np.array([]) for el in KEYLIST]
    for threedayarray in fulldataarray:
        times = threedayarray[0]
        X_data = np.asarray(threedayarray[1], dtype=int)
        Y_data = np.asarray(threedayarray[2], dtype=int)
        if len(X_data) == 4320 and len(X_data) == len(Y_data):
            if debug:
                print("Got valid data - runnig analysis")
            # Initialize global variables
            kfmi = k_fmi(step_size=step_size, K9_limit=K9_limit, longitude=longitude, missing_data=missing_data)
        elif len(X_data) > 0 and not len(X_data) == 4320:
            print("Invalid data length")
            return DataStream()
        elif not len(X_data) > 0 and not test:
            print("Did not find valid data - aborting")
            return DataStream()
        elif test:
            print("---------------------------------")
            print("Runnig test mode with random data")
            debug = True
            x = np.random.uniform(209500, 210000, size=(72, 1))
            X_data = np.tile(x, (1, 1 * 60)).flatten()
            y = np.random.uniform(29500, 30000, size=(36, 1))
            Y_data = np.tile(y, (1, 2 * 60)).flatten()
            kfmi = k_fmi(step_size=60, K9_limit=500, longitude=longitude, missing_data=missing_data)

        # Step 0: Fill gaps with means
        T_table = kfmi.GetTimes(times)
        X_data = kfmi.fill_gaps(X_data)
        Y_data = kfmi.fill_gaps(Y_data)

        # Step 1: Compute preliminary K indices by max-min method
        K_table = kfmi.FillKTable(X_data, Y_data, Subtract=False)
        if debug:
            print("K_table - first estimate - plain means", K_table)

        # Step 2: Compute the fitting lengths and find average values for each hour
        FitLength = kfmi.FindLengths(K_table)
        X_mean, Y_mean = kfmi.ComputeHourAverages(X_data, Y_data, FitLength)

        # Step 3: Compute the harmonic fit to hour averages
        X_mean, Y_mean, X_harm, Y_harm = kfmi.ComputeHarmonicFit(X_mean, Y_mean)

        # Step 4: Compute new K values from Original data - Harmonic fit
        K_table = kfmi.FillKTable(X_data, Y_data, X_harm, Y_harm, Subtract=True)
        if debug:
            print("K_table - second estimate - harmonic fit", K_table)

        # Step 5: Compute the fitting lengths and find average values for each hour
        FitLength = kfmi.FindLengths(K_table)
        X_mean, Y_mean = kfmi.ComputeHourAverages(X_data, Y_data, FitLength)

        # Step 6: Compute the harmonic fit to hour averages
        X_mean, Y_mean, X_harm, Y_harm = kfmi.ComputeHarmonicFit(X_mean, Y_mean)

        # Step 7: Compute final K values from Original data - Harmonic fit
        K_table = kfmi.FillKTable(X_data, Y_data, X_harm, Y_harm, Subtract=True)
        if debug:
            print("K_table - final estimate - harmonic fit", K_table)
            print("---------------------------------")

        tresult.extend(T_table)
        kresult.extend(K_table)

    array[0] = np.asarray(tresult)
    ind = KEYLIST.index('var1')
    array[ind] = np.asarray(kresult)
    header = copy.deepcopy(datastream.header)
    header['DataSamplingRate'] = 10800
    header['StationK9'] = K9_limit
    header['DataSamplingFilter'] = 'FMI K determination'

    return DataStream([], header, np.asarray(array, dtype=object))

try:
    import emd
    emdpackage = True

    class decompose(object):
        """
        DESCRIPTION
            Class to decompse any given signal into frequency bands using a emperical mode decomposition, The frequency bands
            are analyzed using a Hilbert_Huang transform.
        REQUIREMENTS
            emd package for emperical mode decomposition
        APPLICATION

        """

        def __init__(self, sample_frequ=1 / 60., max_imfs=16, imf_opts={'sd_thresh': 0.1}, nensembles=24, nprocesses=6,
                     ensemble_noise=1):
            self.sample_frequ = sample_frequ
            self.max_imfs = max_imfs
            self.nensembles = nensembles
            self.nprocesses = nprocesses
            self.ensemble_noise = ensemble_noise

        def normalize_component(self, comp):
            comp = comp - np.nanmean(comp)
            return comp

        def emd_sift(self, comp, sift_type='mask', debug=False):
            if sift_type == 'mask':
                imf = emd.sift.mask_sift(comp, max_imfs=self.max_imfs)
            elif sift_type == 'ensemble':
                imf = emd.sift.ensemble_sift(comp, max_imfs=self.max_imfs, nensembles=24, nprocesses=6, ensemble_noise=1,
                                             imf_opts=imf_opts)
            else:
                imf = emd.sift.sift(comp, max_imfs=16, imf_opts=imf_opts)
            if debug:
                emd.plotting.plot_imfs(imf)
            IP, IF, IA = emd.spectra.frequency_transform(imf, self.sample_frequ, 'nht')
            return imf, IP, IF, IA

        def frequency_anaylsis(self, IF, debug=False):
            # Frequency and Periods
            imf_stats_dict = {}
            p_dict = {}
            f_dict = {}
            for i in range(shape(IF)[1]):
                mu, std = norm.fit(IF[:, i])
                x = np.linspace(0, 0.0001)
                p = norm.pdf(x, mu, std)
                if debug:
                    print(
                        "Peakperiod of IMF-{}: {:.1f} hours ( {:.1f} days or {:.1f} minutes )".format(i + 1, 1 / mu / 3600,
                                                                                                      1 / mu / 86400,
                                                                                                      1 / mu / 60))
                p_dict[i] = 1 / mu
                f_dict[i] = mu
            imf_stats_dict["period"] = p_dict
            imf_stats_dict["frequency"] = f_dict
            return imf_stats_dict


    class quietday(object):
        """
        DESCRIPTION
            class to determine a mainly frequency dependend quiet day curve for activity estimates
        REQUIREMENTS
            emd package for emperical mode decomposition
        """

        def __init__(self, sample_frequ=1 / 60., max_imfs=16, imf_opts={'sd_thresh': 0.1}, nensembles=24, nprocesses=6,
                     ensemble_noise=1):
            self.sample_frequ = sample_frequ
            self.max_imfs = max_imfs
            self.nensembles = nensembles
            self.nprocesses = nprocesses
            self.ensemble_noise = ensemble_noise

        def disturbed_regions(self, comp, IA, IF, f=1.5, n_imf=5, debug=False):
            # Getting doistrubed data based on amplitude exceeding threshold on IMF 1 (~8h Period)
            dimf = IA[:, n_imf]

            Q1 = np.nanquantile(dimf, 0.25)
            Q3 = np.nanquantile(dimf, 0.75)
            IQR = Q3 - Q1
            # define an upper limit, for which amplitudes exceeding this limit are definitly indicating disturbed data
            ul = Q3 + f * IQR
            if debug:
                print(Q1, Q3, IQR, ul)

            mu, std = norm.fit(IF[:, n_imf])
            x = np.linspace(0, 0.0001)
            p = norm.pdf(x, mu, std)
            if debug:
                print("Peakperiod: {} hours".format(1 / mu / 3600))

            # gap analysis - get good and bad indicies - create bad windows array
            exceedinginds = np.argwhere(dimf > ul)
            disturbed_minutes = len(exceedinginds)
            peakperiod_min = 1 / mu / 60
            if debug:
                print("Disturbed minutes:", disturbed_minutes)
            disturbed_regions = []
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
            if debug:
                print("Amount of disturbed regions", len(disturbed_regions))
            # Extend the size of each region by periods width before and after to be sure that beginning and end of disturbance is grapped
            extender = int(peakperiod_min)
            if debug:
                print(extender)
            ndisturbed_regions = []
            win_stats = []
            for el in disturbed_regions:
                start = el[0] - extender
                end = el[-1] + extender
                if start < 0:
                    start = 0
                if end > len(comp) - 1:
                    end = len(comp) - 1
                win = list(range(start, end + 1))
                win_stats.append(len(win))
                ndisturbed_regions.append(win)
            # List the average, maximal and mininmal lengths of disturbed ranges
            if debug:
                print("Window minimal length (hours)", min(win_stats) / 60)
                print("Window maximal length (hours)", max(win_stats) / 60)
                print("Window average length (hours)", mean(win_stats) / 60)
            # Combine eventually overlapping regions later with cycle
            return ndisturbed_regions

        def create_mask(self, comp, ndisturbed_regions, IP, n_imf=8):
            C = emd.cycles.Cycles(IP[:, n_imf])
            l = C.metrics['is_good']
            good = []
            for ii, el in enumerate(l):
                if el:
                    good.append(ii)
            cycles_to_plot = good
            # get a mask for all bad data and also a array for plotting shaded boxes with bad data
            fullmask = np.full(len(comp), True)
            for ii in range(len(cycles_to_plot)):
                currinds = C.get_inds_of_cycle(cycles_to_plot[ii])
                truearray = np.full(len(currinds), False)
                np.put(fullmask, currinds, truearray)

            for currinds in ndisturbed_regions:
                falsearray = np.full(len(currinds), True)
                np.put(fullmask, currinds, falsearray)
            return fullmask

        def waveform(self, imf, IP, IA, imf_stats, fullmask, debug=False):
            # cycle waveform  - use mask from disturbed indicies and only use last 27 days (and next 27 days in one year)
            # go from cycle to cycle
            all_cycles = emd.cycles.get_cycle_vector(IP, return_good=False)

            p_dict = imf_stats.get("period")
            len_imf = shape(IP)[1]  # ranging from 1 tp len_imf
            if len_imf <= 6:
                return

            disturbed_mask = IA[:, 8] > .05
            gapadoption = 'linear'

            # go through all cycle below minute frequ 11
            max_range_cycle = 11
            if len_imf < 11:
                max_range_cycle = len_imf
            cycle_range = 13  # i.e. 27 days 13 + 1 + 13
            waveformdict = {}
            orgwaveformdict = {}
            for n_imf in range(6, max_range_cycle):
                if debug:
                    print("Analyzing average cycle for IMF-{}".format(n_imf + 1))
                mean_imf = np.zeros(len(imf[:, n_imf])) * np.nan
                Ci = emd.cycles.Cycles(IP[:, n_imf])
                n_cycles = all_cycles[:, n_imf].max()
                if debug:
                    print(" - N of cycles", n_cycles)
                if n_imf > 8:
                    cycle_range = int(cycle_range / 2)
                for n, cycle in enumerate(Ci):
                    cycledict = {}
                    # print ("    running cycle", n)
                    newmask = [el for el in disturbed_mask]
                    currinds = Ci.get_inds_of_cycle(n)
                    # print (inds)
                    # eventually reduce the cycle range for keep 27 days above IMF-9
                    upper_limit = n + cycle_range
                    lower_limit = n - cycle_range
                    first_valid = 99999999
                    last_valid = 0
                    if lower_limit >= 0:
                        # get last index of
                        inds = Ci.get_inds_of_cycle(lower_limit)
                        if inds.any() and len(inds) > 0:
                            first_valid = inds[0]
                    else:
                        inds = Ci.get_inds_of_cycle(0)
                        if inds.any() and len(inds) > 0:
                            first_valid = inds[0]
                    if upper_limit > 0 and upper_limit < n_cycles:
                        inds = Ci.get_inds_of_cycle(upper_limit)
                        if inds.any() and len(inds) > 0:
                            last_valid = inds[-1]
                    else:
                        inds = Ci.get_inds_of_cycle(n_cycles)
                        if inds.any() and len(inds) > 0:
                            last_valid = inds[-1]
                    # print ("    First and last valid",first_valid, last_valid)
                    if not last_valid == 0 and not first_valid == 99999999:
                        newmask = np.array(
                            [False if i < first_valid or i >= last_valid else el for i, el in enumerate(newmask)])
                    else:
                        newmask = newmaskprev
                    newmaskprev = newmask
                    cycles_tobeused = emd.cycles.get_cycle_vector(IP, return_good=True, mask=newmask)
                    period = p_dict.get(n_imf) / 60
                    win_len = int(2 * period)
                    # print (period)
                    waveform = np.zeros((win_len, cycles_tobeused.max())) * np.nan
                    for ii in range(1, cycles_tobeused.max() + 1):
                        inds = cycles_tobeused[:, n_imf] == ii
                        waveform[:np.sum(inds), ii - 1] = imf[inds, n_imf]
                    # print (len(waveform), currinds[0],currinds[-1])
                    np.put(mean_imf, currinds, np.nanmedian(waveform, axis=1))
                    # print (mean_imf)
                    if n == 0 and debug:
                        plt.title('Linear avg. waveform')
                        plt.plot(np.nanmean(waveform, axis=1))
                        plt.xticks(np.arange(5) * 400, [])
                        plt.grid(True)
                # fit meadin_imf with spline:
                amount = len(mean_imf)
                t = np.linspace(0, amount, amount)
                # Asign zero weigths to nan values
                w = np.isnan(mean_imf)
                mean_imf[w] = 0.
                spl = UnivariateSpline(t, mean_imf, w=~w)
                orgwaveformdict[n_imf] = mean_imf
                waveformdict[n_imf] = spl(t)

            for n_imf in range(11, len_imf):
                if debug:
                    print("Interpolating gaps for IMF-{}".format(n_imf + 1))
                # remove disturbed regions
                ma = np.ma.masked_array(data=imf[:, n_imf], mask=fullmask)
                ma_filled = ma.filled(np.nan)
                if gapadoption == 'linear':
                    # linear interpolation
                    nans, x = nan_helper(ma_filled)
                    ma_filled[nans] = np.interp(x(nans), x(~nans), ma_filled[~nans])
                    waveformdict[n_imf] = ma_filled
                else:
                    # interpolate nan values using cubic spline or line?
                    amount = len(ma_filled)
                    t = np.linspace(0, amount, amount)
                    # Asign zero weigths to nan values
                    w = np.isnan(ma_filled)
                    ma_filled[w] = 0.
                    spl = UnivariateSpline(t, ma_filled, w=~w)
                    waveformdict[n_imf] = spl(t)
            return waveformdict

        def weight_func(self, fullmask, w=720):
            # w is windowlength in minutes, default 720 minutes == 12h
            double = fullmask.astype(int) * 2
            weightfunc = np.convolve((double), np.ones(w), 'same') / w
            weightfunc[weightfunc > 1] = 1
            return weightfunc

        def joint_bl(self, comp, emd_baseline, median_baseline, weightfunc):
            min = range(0, len(comp))
            joint_baseline = np.zeros(len(comp)) * np.nan
            for i in min:
                vals = [emd_baseline[i], median_baseline[i]]
                weights = np.array([1 - weightfunc[i], weightfunc[i]])
                joint_baseline[i] = np.average(vals, weights=weights)
            return joint_baseline


    def emd_decompose(onedarray, sift_type='mask', sample_frequ=1 / 60., max_imfs=16, imf_opts={'sd_thresh': 0.1},
                      nensembles=24, nprocesses=6, ensemble_noise=1, debug=False):
        """
        DESCRIPTION
            Decompose any given signal into ferquency bands
        """
        dc = decompose(sample_frequ=sample_frequ, max_imfs=max_imfs, imf_opts=imf_opts, nensembles=nensembles,
                       nprocesses=nprocesses, ensemble_noise=ensemble_noise)
        comp = dc.normalize_component(onedarray)
        imf, IP, IF, IA = dc.emd_sift(comp, sift_type=sift_type, debug=debug)
        stats = dc.frequency_anaylsis(IF, debug=debug)
        return comp, imf, IP, IF, IA, stats


    def sqbase(datastream, components=['x'], baseline_type='emd', sift_type='mask', sample_frequ=1 / 60., max_imfs=16,
               imf_opts={'sd_thresh': 0.1}, nensembles=24, nprocesses=6, ensemble_noise=1, debug=False):
        """
        DESCRIPTION
            Feed at least 1 month of one-miute data into this function.
            Three different types of quiet day baselines can be obtained
            emd : baseline based on emperical mode decomposition corresponding to a low pass approximately above 3h
            median : baseline based on average cycle and and its frequency dependend median waveform
            joint : baseline combination of emd and median. emd is used in for time ranges assumed to be undisturbed.
                    Disturbed regions (geomag storms etc) are masked and filled median baseline is used there.
                    A weighting function is used for smooth emd - median -emd conversions
        PARAMETERS
            datastream
            components
            baseline_type
            sift_type='mask'
            sample_frequ=1/60.
            max_imfs=16
            imf_opts={'sd_thresh': 0.1}
            nensembles=24
            nprocesses=6
            ensemble_noise=1
        RETURNS
            a datastream with quiet day baselines for all selected components
        APPLICATION
            sqvariation = sqbase(teststream, components=['x'], baseline_type='joint')

        """
        emd_baseline = None
        median_baseline = None
        # step1 - use minute data
        sr = datastream.samplingrate()
        if not sr <= 60.3:
            return
        elif sr < 59.7:
            datastream = datastream.filter()
        header = copy.deepcopy(datastream.header)
        t = datastream.ndarray[0]
        array = [np.asarray([]) for el in KEYLIST]
        # step2 - test datastream vailidity
        qd = quietday()
        # step3 - extract components
        for c in components:
            compindex = KEYLIST.index(c)
            comp = datastream._get_column(c)
            cmean = np.nanmean(comp)
            if debug:
                print("Amount of data:", len(comp))
            ncomp, imf, IP, IF, IA, stats = emd_decompose(comp, sift_type=sift_type, sample_frequ=sample_frequ,
                                                          max_imfs=max_imfs, imf_opts=imf_opts, nensembles=nensembles,
                                                          nprocesses=nprocesses, ensemble_noise=ensemble_noise)
            emd_baseline = imf[:, 6]
            for i in range(7, shape(IP)[1]):
                emd_baseline += imf[:, i]
            if baseline_type in ['median', 'joint']:
                ndisturbed_regions = qd.disturbed_regions(ncomp, IA, IF)
                mask = qd.create_mask(ncomp, ndisturbed_regions, IP)
                waveform = qd.waveform(imf, IP, IA, stats, fullmask=mask, debug=debug)
                median_baseline = waveform.get(6)
                for i in range(7, shape(IP)[1]):
                    median_baseline += waveform.get(i)
                if baseline_type in ['joint']:
                    weightfunc = qd.weight_func(fullmask=mask)
                    baseline = qd.joint_bl(ncomp, emd_baseline, median_baseline, weightfunc)
                else:
                    baseline = median_baseline
            else:
                baseline = emd_baseline
            array[compindex] = baseline + cmean

        array[0] = t
        header['SensorID'] = "BL{}-{}".format(baseline_type, datastream.header.get('SensorID'))

        return DataStream([], header, np.asarray(array, dtype=object))
except:
    print (" - install the emd package to use quiet day baseline estimates")
    emdpackage = False
    pass

if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: ACTIVITY PACKAGE")
    print("THIS IS A TEST RUN OF THE MAGPY ACTIVITY PACKAGE.")
    print("All main methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()

    # Creating a test data set of second resolution and 6 day length
    c = 4000 # 4000 nan values are filled at random places to get some significant data gaps
    array = [[] for el in KEYLIST]
    x = np.random.uniform(20950, 21000, size=(288, 1))
    x = np.tile(x, (1, 60*60)).flatten()
    x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
    array[1] = x
    y = np.random.uniform(1950, 2000, size=(288, 1))
    y = np.tile(y, (1, 60*60)).flatten()
    y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
    array[2] = y
    z = np.random.uniform(44300, 44400, size=(288, 1))
    z = np.tile(z, (1, 60*60)).flatten()
    array[3] = z
    array[0] = np.asarray([datetime(2022,11,21)+timedelta(seconds=i) for i in range(0,len(z))])
    teststream = DataStream([],{'SensorID':'Test_0001_0001'},np.asarray(array, dtype=object))

    errors = {}
    successes = {}
    testrun = 'streamtestfile'
    t_start_test = datetime.utcnow()
    while True:
        try:
            ts = datetime.utcnow()
            minstream = teststream.filter()
            k = K_fmi(minstream, debug=False)
            te = datetime.utcnow()
            successes['K_fmi'] = (
                "Version: {}, K_fmi: {}".format(magpyversion, (te - ts).total_seconds()))
        except Exception as excep:
            errors['K_fmi'] = str(excep)
            print(datetime.utcnow(), "--- ERROR determining K_fmi.")
        if emdpackage:
            try:
                ts = datetime.utcnow()
                minstream = teststream.filter(missingdata='interploate')
                bs = qdbase(minstream, components=['z'], baseline_type='')
                te = datetime.utcnow()
                successes['qdbaseline'] = (
                    "Version: {}, qdbaseline: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['qdbaseline'] = str(excep)
                print(datetime.utcnow(), "--- ERROR determining quiet day baseline")

        break

    t_end_test = datetime.utcnow()
    time_taken = t_end_test - t_start_test
    print(datetime.utcnow(), "- Stream testing completed in {} s. Results below.".format(time_taken.total_seconds()))

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
