# -*- coding: utf-8 -*-

"""
activity contain all methods for geomagnetic activity analysis

| class | method | since version | until version | runtime test | result verification | manual | *tested by |
| ----- | ------ | ------------- | ------------- | ------------ | ------------------- | ------ | ---------- |
| **core.activity** |  |         |               |              |                    |        | |
| core.conversions |  |          |               |              |                    |        | |
| decompose |    | 2.0.0         |               | yes*         |                    |  8.3   | sqbase |
| k_fmi |        | 2.0.0         |               | yes*         |  yes*              |  8.1   | K_fmi |
| stormdet |     | 2.0.0         |               | yes*         |  yes*              |  8.2   | seek_storm |
|       | emd_decompose | 2.0.0  |               | yes*         |                    |  8.3   | sqbase |
|       | K_fmi | 2.0.0          |               | yes          |  yes               |  8.1   | |
|       | seek_storm | 2.0.0     |               | yes          |  yes               |  8.2   | |
|       | sqbase | 2.0.0         |               | yes          |  no                |  8.3   | requires long test set |


"""

import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import *
from magpy.core.methods import find_nearest
import copy
from scipy.stats import norm
from scipy.interpolate import UnivariateSpline

class k_fmi(object):
    """
        Checkout K_fmi for details on this class
        import magpy.core.activity
        help(K_fmi)
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
        for hour3 in range(0, 8):
            MiddlePoint = self.pointsinday + (180 * (60 * hour3 + 30)) // self.step_size
            tr.append(Time_array[MiddlePoint])
        return tr


def K_fmi(datastream, step_size=60, K9_limit=750, longitude=15.0, missing_data=999999, return_sq=False, test=False, debug=False):
    """
    DESCRIPTION:
        determination of K values based on the FMI method. This class is derived from the original C code
        K_index.h by Lasse  Hakkinen, Finnish Meteorological Institute.
        (https://space.fmi.fi/MAGN/K-index/FMI_method/K_index.h) Please note: the original method
        fills data gaps based on mean values if gaps are smaller than a given maximum gap length.
        Details on the procedure can be found here:
        Sukksdorf, Pirola, Häkkinen, 1991. Computer production of K indices based on linear elimination
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
        return_sq    : instead of K values the Sq curve (harmonic fit will be
                     returned.
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

    # If defaults then check and eventually override by header content
    if K9_limit == 750 and datastream:
        K9_limit = int(datastream.header.get('StationK9', 750))
    if longitude == 15.0 and datastream:
        longitude = float(datastream.header.get('DataAcquisitionLongitude', 15.0))

    tresult = []
    kresult = []
    sq_k_t = []
    sq_k_x = []
    sq_k_y = []
    array = [np.array([]) for el in DataStream().KEYLIST]
    for threedayarray in fulldataarray:
        times = threedayarray[0]
        X_data = np.asarray(threedayarray[1], dtype=int)
        Y_data = np.asarray(threedayarray[2], dtype=int)
        kfmi = k_fmi()
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
            times = np.asarray([datetime(2022,11,21)+timedelta(minutes=i) for i in range(0,len(X_data))])
            datastream = DataStream()
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
        if return_sq:
            sqt = times[kfmi.pointsinday:2 * kfmi.pointsinday]
            sq_k_t.extend(sqt)
            sq_k_x.extend(X_harm/10.)
            sq_k_y.extend(Y_harm/10.)

    if not return_sq:
        array[0] = np.asarray(tresult)
        ind = DataStream().KEYLIST.index('var1')
        array[ind] = np.asarray(kresult)
        header = copy.deepcopy(datastream.header)
        header['DataSamplingRate'] = 10800
        header['StationK9'] = K9_limit
        header['DataSamplingFilter'] = 'FMI K determination'
        header['DataType'] = 'MagPyK 2.0'
        header['col-var1'] = 'K'
    else:
        array[0] = np.asarray(sq_k_t)
        ind = DataStream().KEYLIST.index('x')
        array[ind] = np.asarray(sq_k_x)
        ind = DataStream().KEYLIST.index('y')
        array[ind] = np.asarray(sq_k_y)
        header = {}
        header['DataSamplingRate'] = 3600
        header['StationK9'] = K9_limit
        header['DataSamplingFilter'] = 'Harmonic Fit - FMI K determination'
        header['DataType'] = 'Sq FMI'
        header['col-x'] = 'X'
        header['col-y'] = 'Y'
        header['unit-col-x'] = 'nT'
        header['unit-col-y'] = 'nT'

    return DataStream(header=header, ndarray=np.asarray(array, dtype=object))


try:
    import emd
    emdpackage = True

    class Decompose(object):
        """
        DESCRIPTION
            Class to decompse any given signal into frequency bands using a emperical mode decomposition, The frequency bands
            are analyzed using a Hilbert_Huang transform.
        REQUIREMENTS
            emd package for emperical mode decomposition
        APPLICATION

        """

        def __init__(self, sample_frequ=1 / 60., max_imfs=16, imf_opts=None, nensembles=24, nprocesses=6,
                     ensemble_noise=1):
            self.sample_frequ = sample_frequ
            self.max_imfs = max_imfs
            self.nensembles = nensembles
            self.nprocesses = nprocesses
            self.ensemble_noise = ensemble_noise
            self.imf_opts = imf_opts if imf_opts else {'sd_thresh': 0.1}

        def normalize_component(self, comp):
            comp = comp - np.nanmean(comp)
            return comp

        def emd_sift(self, comp, sift_type='mask', debug=False):
            if sift_type == 'mask':
                imf = emd.sift.mask_sift(comp, max_imfs=self.max_imfs)
            elif sift_type == 'ensemble':
                imf = emd.sift.ensemble_sift(comp, max_imfs=self.max_imfs, nensembles=24, nprocesses=6, ensemble_noise=1,
                                             imf_opts=self.imf_opts)
            else:
                imf = emd.sift.sift(comp, max_imfs=16, imf_opts=self.imf_opts)
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


    class QuietDay(object):
        """
        DESCRIPTION
            class to determine a mainly frequency dependend quiet day curve for activity estimates
        REQUIREMENTS
            emd package for emperical mode decomposition
        """

        def __init__(self, sample_frequ=1 / 60., max_imfs=16, imf_opts=None, nensembles=24, nprocesses=6,
                     ensemble_noise=1):
            self.sample_frequ = sample_frequ
            self.max_imfs = max_imfs
            self.nensembles = nensembles
            self.nprocesses = nprocesses
            self.ensemble_noise = ensemble_noise
            self.imf_opts = imf_opts if imf_opts else {'sd_thresh': 0.1}


        def disturbed_regions(self, comp, IA, IF, f=1.5, n_imf=5, debug=False):
            # Getting doistrubed data based on amplitude exceeding threshold on IMF 1 (~8h Period)
            win = 0
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
            newmaskprev = []
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


    def emd_decompose(onedarray, sift_type='mask', sample_frequ=1 / 60., max_imfs=16, imf_opts=None,
                      nensembles=24, nprocesses=6, ensemble_noise=1, debug=False):
        """
        DESCRIPTION
            Decompose any given signal into ferquency bands
        """
        imf_opts = imf_opts if imf_opts else {'sd_thresh': 0.1}

        dc = Decompose(sample_frequ=sample_frequ, max_imfs=max_imfs, imf_opts=imf_opts, nensembles=nensembles,
                       nprocesses=nprocesses, ensemble_noise=ensemble_noise)
        comp = dc.normalize_component(onedarray)
        imf, IP, IF, IA = dc.emd_sift(comp, sift_type=sift_type, debug=debug)
        stats = dc.frequency_anaylsis(IF, debug=debug)
        return comp, imf, IP, IF, IA, stats


    def sqbase(datastream, components=None, baseline_type='emd', sift_type='mask', sample_frequ=1 / 60., max_imfs=16,
               imf_opts=None, nensembles=24, nprocesses=6, ensemble_noise=1, debug=False):
        """
        DESCRIPTION
            Feed at least 1 month of one-minute data into this function.
            Three different types of quiet day baselines can be obtained
            emd : baseline based on empirical mode decomposition corresponding to a low pass approximately above 3h
            median : baseline based on average cycle and and its frequency dependent median waveform
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
        components = components if components else ['x']
        imf_opts = imf_opts if imf_opts else {'sd_thresh': 0.1}
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
        array = [np.asarray([]) for el in datastream.KEYLIST]
        # step2 - test datastream vailidity
        qd = QuietDay()
        # step3 - extract components
        for c in components:
            compindex = datastream.KEYLIST.index(c)
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


class StormDet(object):

    def __init__(self, funcvars=None, d_amp_min=5, ace_window=30, a_varr=-0.000064328748639, b_varr=0.0599247768):

        # Normal variables:
        # Picked out using SD_VarOptimise on methods tested on all storms (vartests):
        if not funcvars:
            funcvars = {'AIC': [4., 3., 20],
                        'DWT2': [0.000645, 60],  # <-- OFFICIAL 100% VALUES #[0.0045, 45], #
                        'DWT1': [2.4499e-05, 65],  # [3.89999e-05, 60] (for 14)
                        'MODWT': [0.0012, 65],  # [3.89999e-05, 60] (for 14)
                        'FDM': [0.0005, 55],
                        'JDM': [11, 1]}
        self.funcvars = funcvars
        # Minimum for storm detection:
        self.d_amp_min = d_amp_min  # nT (WEAK)
        # ACE Variables:
        self.ace_window = ace_window  # 0.5x window of minutes around estimated arrival time for SSC
        # Calculation of arrival time:
        self.a_varr = a_varr  # -5.18166868e-5
        self.b_varr = b_varr  # 0.0560048791

    # ********************************************************************
    #    SUPPORTING FUNCTIONS FOR seekStorm:                            #
    #       checkACE()                                                  #
    #       findSSC()                                                   #
    #       findSSC_AIC()                                               #
    # ********************************************************************

    def checkACE(self, ACE_1m, ACE_5m=None, acevars=None, timestep=20, lastcompare=20,
                 vwind_bracket=None, vwind_weight=1., dvwind_bracket=None, dvwind_weight=1.,
                 stdlo_bracket=None, stdlo_weight=1., stdhi_bracket=None, stdhi_weight=1.,
                 pflux_bracket=None, pflux_weight=4., verbose=False):
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
            ACE_det, ACE_ssc_list = checkACE(ACE_1m, ACE_5m=ACE_5m, verbose=True)
        '''

        acevars = acevars if acevars else {'1m': 'var2', '5m': 'var1'}
        vwind_bracket = vwind_bracket if vwind_bracket else [380., 450.]
        dvwind_bracket = dvwind_bracket if dvwind_bracket else [40., 80.]
        stdlo_bracket = stdlo_bracket if stdlo_bracket else [15., 7.5]
        stdhi_bracket = stdhi_bracket if stdhi_bracket else [0.5, 2.]
        pflux_bracket = pflux_bracket if pflux_bracket else [10000., 50000.]

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
        flux_var = 0.
        pflux_prob = 0.

        t_ind = DataStream().KEYLIST.index('time')
        nantest = ACE_1m._get_column(key_s)
        # nantest = ACE_1m.ndarray[s_ind]
        ar = [elem for elem in nantest if not isnan(elem)]
        div = float(len(ar)) / float(len(nantest)) * 100.0
        if div < 10.:
            raise Exception("Too many NaNs in ACE SWEPAM data.")

        # CALCULATE VARIABLES FOR USE IN EVALUATION:
        # ------------------------------------------
        dACE = self._calcDVals(ACE_1m, key_s, lastcompare, timestep)

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
            div = float(len(ar)) / float(len(dv_test)) * 100.0
            if div < 5.:
                if verbose == True:
                    print("Too many nans!")
                break

            # Find max in change of mean (if dv_max > 25 --> detection!):
            i_max = np.nanargmax(dv_test)
            dv_max, t_max = dv_test[i_max], t_test[i_max]
            try:
                v_max = v_test[i_max + 5]
                i = 1
                while True:
                    if isnan(v_max):
                        v_max = v_test[i_max + 5 + i]
                    else:
                        break
                    # logger.info("checkACE: Too many v_wind NaNs... searching for value...")
                    i += 1
            except:
                v_max = v_test[i_max]

            # CALCULATE PROB THAT DETECTION IS REAL STORM:
            # --------------------------------------------
            if dv_max >= 25.:

                # logger.info("checkACE: Found a possible detection...")
                t_lo = t_max - timedelta(minutes=(timestep + 20))
                t_hi = t_max + timedelta(minutes=(lastcompare + 20))
                dACE_lo = dACE.trim(starttime=t_lo, endtime=t_max)
                dACE_hi = dACE.trim(starttime=t_max, endtime=t_hi)

                std_lo = dACE_lo.mean('var4')
                std_hi = dACE_hi.mean('var4')

                # 1ST REQUIREMENT:
                # ****************
                # A storm is most likely at higher wind speeds (v > 450.)
                # Solar wind speed at max:
                if v_max < vwind_bracket[0]:
                    v_prob = 50.
                elif vwind_bracket[0] <= v_max < vwind_bracket[1]:
                    v_prob = 75.
                elif v_max > vwind_bracket[1]:
                    v_prob = 100.
                else:
                    v_prob = 50.
                v_prob = v_prob * vwind_weight

                # 2ND REQUIREMENT:
                # ****************
                # Must be a sudden jump (discontinuity) in the data.
                # Delta-v amplitude:
                if dv_max < dvwind_bracket[0]:
                    dv_prob = 50.
                elif dvwind_bracket[0] <= dv_max < dvwind_bracket[1]:
                    dv_prob = 75.
                elif dv_max > dvwind_bracket[1]:
                    dv_prob = 100.
                else:
                    dv_prob = 50.
                dv_prob = dv_prob * dvwind_weight

                # 3RD REQUIREMENT:
                # ****************
                # Check for a change in std. dev. after the discontinuity.
                # Std dev. (BEFORE discontinuity):
                if std_lo > stdlo_bracket[0]:
                    stlo_prob = 50.
                elif stdlo_bracket[1] <= std_lo < stdlo_bracket[0]:
                    stlo_prob = 75.
                elif std_lo < stdlo_bracket[1]:
                    stlo_prob = 100.
                else:
                    stlo_prob = 50.
                stlo_prob = stlo_prob * stdlo_weight

                # Std dev. (AFTER discontinuity):
                if std_hi < stdhi_bracket[0] * std_lo:
                    sthi_prob = 50.
                elif stdhi_bracket[0] * std_lo <= std_hi < stdhi_bracket[1] * std_lo:
                    sthi_prob = 75.
                elif std_hi >= stdhi_bracket[0] * std_lo:
                    sthi_prob = 100.
                else:
                    sthi_prob = 50.
                sthi_prob = sthi_prob * stdhi_weight

                # Calculate final probability:
                total_weight = vwind_weight + dvwind_weight + stdlo_weight + stdhi_weight
                probf = (v_prob + dv_prob + stlo_prob + sthi_prob) / total_weight

                if verbose == True:
                    print(t_max, v_max, dv_max, std_lo, std_hi)
                    print(t_max, v_prob, dv_prob, stlo_prob, sthi_prob, '=', probf)

                # Detection is likely a storm if total probability is >= 70.
                # if probf >= 70.:
                # Check ACE EPAM data for rise in solar proton flux
                if ACE_5m != None:
                    t_5m = ACE_5m._get_column('time')
                    t_val, idx = find_nearest(t_5m, t_max)
                    start, end = (t_val,
                                  t_val + timedelta(minutes=10))
                    # TODO: Figure out why this function still decimates ACE_5m
                    ACE_flux = ACE_5m.trim(starttime=start, endtime=end)
                    flux_val = ACE_flux.mean(key_e, percentage=20)
                    if isnan(flux_val):
                        # First try larger area:
                        ACE_flux = ACE_5m.trim(starttime=start - timedelta(minutes=10),
                                               endtime=(end + timedelta(minutes=20)))
                        flux_val = ACE_flux.mean(key_e, percentage=20)
                        if isnan(flux_val):
                            # logger.warning("checkACE: Proton Flux is nan!", flux_val)
                            flux_val = 0.
                            # raise Exception
                    # logger.info("--> Jump in solar wind speed detected! Proton flux at %s." % flux_val)

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
                    probf = (probf + pflux_prob) / (total_weight + pflux_weight)
                    flux_var = flux_val

                elif ACE_5m == None:
                    flux_var = 0.

                acedict = {}
                # CALCULATE ESTIMATED ARRIVAL TIME
                satssctime = t_max
                # a_arr = -0.0000706504807969411
                # b_arr = 0.0622525912
                # arr_est = a_arr * v_max + b_arr # original
                # estssctime = satssctime + timedelta(minutes=(arr_est * 60. * 24.))
                a_arr = 2.29684514e+04
                b_arr = -7.62595734e+00
                arr_est = a_arr * (1. / v_max) + b_arr
                estssctime = satssctime + timedelta(minutes=arr_est)
                acedict['satssctime'] = satssctime
                acedict['estssctime'] = estssctime
                acedict['vwind'] = v_max
                acedict['pflux'] = flux_var
                acedict['probf'] = probf
                detection = True
                ace_ssc.append(acedict)

                if verbose:
                    print("Removing data from {} to {}.".format(t_max - timedelta(minutes=15 + timestep),
                                                                t_max + timedelta(minutes=15 + lastcompare)))
                dACE = dACE.remove(starttime=t_max - timedelta(minutes=15 + timestep),
                                   endtime=t_max + timedelta(minutes=15 + lastcompare))

            else:
                break

        # Until boundary effects can be mitigated, this should deal with potentially incorrect detections:
        # TODO: take this out at some point
        if detection:
            if ((ACE_1m.ndarray[t_ind][-1] - ace_ssc[0]['satssctime']).seconds < (timestep / 2.) * 60. or
                    (ace_ssc[0]['satssctime'] - ACE_1m.ndarray[t_ind][0]).seconds < (timestep / 2.) * 60.):
                detection, ace_ssc = False, []

        if verbose == True:
            print("ACE storms detected at:")
            for item in ace_ssc:
                print(item['satssctime'])

        return detection, ace_ssc

    def findSSC(self, var_stream, var_key, a, p, useACE=False, ACE_results=None, dh_bracket=None,
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
            magdata = read(FGE_file)
            satdata_1m = read(ACE_1m_file)
            satdata_5m = read(ACE_5m_file)
            ACE_detection, ACE_results = checkACE(satdata_1m, satdata_5m)
            DWT = magdata.dwt_calc()
            var_key = 'var2'            # use second detail D2
            a, p = 0.0004, 45           # amplitude 0.0004 in D2 var must be exceeded over period 45 seconds
            detection, ssc_list = findSSC(DWT, var_key, a, p, useACE=useACE,
                    ACE_results=ACE_results, verbose=verbose)
        '''
        ACE_results = ACE_results if ACE_results else []
        dh_bracket = dh_bracket if dh_bracket else [5., 10.]

        # CHECK DATA:
        # -----------
        if len(var_stream) == 0:
            raise Exception("Empty stream!")

        possdet = False
        detection = False
        SSC_list = []

        var_ind = DataStream().KEYLIST.index(var_key)
        var_ar = var_stream.ndarray[var_ind]
        t_ind = DataStream().KEYLIST.index('time')
        t_ar = var_stream.ndarray[t_ind]
        x_ind = DataStream().KEYLIST.index('x')
        x_ar = var_stream.ndarray[x_ind]

        i = 0
        x1,x2 = 0,0
        ssc_init = None
        timepin = None
        dh_prob = 0
        final_probf = 0
        det = False

        # SEARCH FOR PEAK:
        # ----------------
        if verbose == True:
            print("Starting analysis with findSSC().")
        # for i in range(0,len(var_ar)):
        while True:
            # var = eval('row.'+var_key)
            var = var_ar[i]

            # CRITERION #1: Variable must exceed the threshold a
            # **************************************************
            if var >= a and possdet == False:
                # timepin = row.time
                timepin = t_ar[i]
                ssc_init = timepin
                # x1 = row.x
                x1 = x_ar[i]
                if verbose:
                    print("x1:", x1, ssc_init)
                possdet = True
            # elif var < a and possdet == True:  # old version. WARNING: Replacement may be buggy.
            elif possdet == True:
                # duration = (num2date(row.time) - num2date(timepin)).seconds
                test_duration = (t_ar[i] - timepin).seconds

                # CRITERION #2: Length of time that variable exceeds a must > p
                # *************************************************************
                if test_duration >= p:
                    # Find full duration:
                    ssc_ends = np.where(var_ar[i:] < a)[0]
                    if len(ssc_ends) == 0:  # not by the end of this time range
                        duration = (t_ar[-1] - timepin).seconds
                    else:
                        i += ssc_ends[0]
                        duration = (t_ar[i] - timepin).seconds
                    # x2 = row.x
                    x2 = x_ar[i]
                    d_amp = x2 - x1
                    if verbose:
                        print("x2:", x2, t_ar[i])
                        print("Possible detection with duration {}} at {}} with {}} nT.".format(duration, ssc_init, d_amp))

                    # CRITERION #3: Variation in H must exceed a certain value
                    # ********************************************************
                    if d_amp > self.d_amp_min:

                        if d_amp >= dh_bracket[0] and d_amp < dh_bracket[1]:
                            dh_prob = 50.
                        if d_amp >= dh_bracket[1]:
                            dh_prob = 100.

                        if verbose == True:
                            print("Detection at %s with %s nT!" % (ssc_init, str(d_amp)))

                        # CRITERION #4: Storm must have been detected in ACE data
                        # *******************************************************
                        if useACE == True and ACE_results != []:

                            # CRITERION #5: ACE storm must have occured 45 (+-20) min before detection
                            # ************************************************************************
                            if verbose:
                                print("Using ACE results to compare SSC time...")
                            for sat_ssc in ACE_results:
                                det, final_probf = self._calcProbWithSat(ssc_init, sat_ssc,
                                                                         dh_prob, dh_weight, satprob_weight,
                                                                         estt_weight, verbose=verbose)
                                if det == True:
                                    if verbose:
                                        print("!!! Matches expected SSC time for SAT results {}".format(ACE_results))
                                    break
                                else:
                                    if verbose:
                                        print("No connection to SAT results {}".format(ACE_results))

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

                    else:
                        if verbose:
                            print("Detection is lower than threshold. Ignoring.")

                    possdet = False

                elif var < a:
                    if verbose:
                        short_duration = (t_ar[i] - timepin).seconds
                        print("Peak fell below threshold. Duration was {:.0f} s".format(short_duration))
                    possdet = False
                else:
                    i += 1


            else:
                i += 1
            if i >= len(var_ar):
                break

        return detection, SSC_list

    def findSSC_AIC(self, stream, aic_key, aic_dkey, mlowval, monsetval, minlen, useACE=False, ACE_results=None,
                    satprob_weight=1., dh_bracket=None, dh_weight=1., estt_weight=2., verbose=False):
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
            magdata = read(FGE_file)
            satdata_1m = read(ACE_1m_file)
            satdata_5m = read(ACE_5m_file)
            ACE_detection, ACE_results = checkACE(satdata_1m, satdata_5m)
            AIC_key = 'var2'
            AIC_dkey = 'var3'
            a_aic, b_aic, minlin = 5., 4., 20
            magdata = magdata.aic_calc('x',timerange=timedelta(hours=0.5),aic2key=AIC_key)
            magdata = magdata.derivative(keys=[AIC_key],put2keys=[AIC_dkey])
            detection, ssc_list = findSSC_AIC(magdata, AIC_key, AIC_dkey, a_aic, b_aic, minlen,
                    useACE=ACE_detection, ACE_results=ACE_results)
        '''
        ACE_results = ACE_results if ACE_results else []
        dh_bracket = dh_bracket if dh_bracket else [5., 10.]

        trange = timedelta(minutes=5.0)  # in minutes

        stream = stream._drop_nans(aic_dkey)

        maxfound = True
        detection = False
        count = 0
        det = False
        final_probf = 0.
        dh_prob = 0.

        aicme, aicstd = stream.mean(aic_key, percentage=10, std=True)
        me, std = stream.mean(aic_dkey, percentage=10, std=True)
        if verbose == True:
            print("AIC Means", aicme, aicstd)
            print("dAIC Means", me, std)

        lowval = std * mlowval
        onsetval = std * monsetval
        minlen = minlen

        t_ind = DataStream().KEYLIST.index('time')
        x_ind = DataStream().KEYLIST.index('x')

        SSC_list = []

        while maxfound:
            maxval, mtime = stream._get_max(aic_dkey, returntime=True)
            if verbose == True:
                print("Max of %s found at %s" % (maxval, mtime))

            # CRITERION #1: Maximum must exceed threshold for a storm
            # *******************************************************
            if maxval > lowval:
                count += 1
                if verbose == True:
                    print("Solution ", count)

                # remaininglst = [elem for elem in stream if elem.time < mtime - trange/60.0/24.0
                # or elem.time > mtime + trange/60.0/24.0]
                remaininglst = stream.copy()
                nst = stream.trim(starttime=mtime - trange, endtime=mtime + trange)
                # plot_new(nst, ['x','var2','var3'])
                for i in range(len(remaininglst.ndarray[t_ind]) - 1, -1, -1):
                    if (remaininglst.ndarray[t_ind][i] > mtime - trange
                            and remaininglst.ndarray[t_ind][i] < mtime + trange):
                        for j in range(0, len(DataStream().KEYLIST)):
                            if len(remaininglst.ndarray[j]) > 0:
                                remaininglst.ndarray[j] = np.delete(remaininglst.ndarray[j], i, 0)
                # plot_new(remaininglst, ['x','var2','var3'])

                daicmin, tmin = nst._get_min(aic_dkey, returntime=True)
                daicmax, tmax = nst._get_max(aic_dkey, returntime=True)
                if verbose == True:
                    print("AIC time min:", tmin)
                    print("AIC time max:", tmax)

                if tmin > tmax:  # Wrong minimum. Fix!
                    while True:
                        nst = nst.trim(endtime=(tmin - timedelta(seconds=1)))
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
                            print("AIC time min:", tmin)
                            print("AIC time max:", tmax)
                        if tmax > tmin:
                            break

                if not maxfound:
                    break

                idx = nst.findtime(tmin)
                # xmin = linemin.x
                xmin = nst.ndarray[x_ind][idx]
                elevatedrange = nst.extract(aic_dkey, onsetval, '>')
                # ssc_init = num2date(linemin.time).replace(tzinfo=None)
                ssc_init = nst.ndarray[t_ind][idx]

                # CRITERION #2: Elevated range must be longer than minlen
                # *******************************************************
                if len(elevatedrange.ndarray[t_ind]) > minlen:
                    # d_amp = elevatedrange[-1].x - xmin
                    d_amp = elevatedrange.ndarray[x_ind][-1] - xmin
                    # duration = (num2date(elevatedrange[-1].time).replace(tzinfo=None)-ssc_init).seconds
                    duration = (elevatedrange.ndarray[t_ind][-1] - ssc_init).total_seconds()

                    if verbose == True:
                        print("Low value:", lowval)
                        print("Onset value:", onsetval)
                        print("Length of onset array:", len(nst))
                        print("Amplitude:", d_amp)
                        print("Max and time:", maxval, tmin)
                        print("Length of elevated range:", len(elevatedrange.ndarray))
                        print("Time of SSC:", ssc_init)

                    # CRITERION #3: Onset in H component must exceed 10nT
                    # ***************************************************
                    if d_amp > self.d_amp_min:

                        if d_amp >= dh_bracket[0] and d_amp < dh_bracket[1]:
                            dh_prob = 50.
                        if d_amp >= dh_bracket[1]:
                            dh_prob = 100.

                        # CRITERION #4: ACE storm must have occured 45 (+-20) min before detection
                        # ************************************************************************
                        if useACE == True and ACE_results != []:
                            for sat_ssc in ACE_results:
                                det, final_probf = self._calcProbWithSat(ssc_init, sat_ssc,
                                                                         dh_prob, dh_weight, satprob_weight,
                                                                         estt_weight, verbose=verbose)
                                if det == True:
                                    break
                        elif useACE == True and ACE_results == []:
                            detection, det = False, False
                            if verbose == True:
                                print("No ACE storm. False detection!")
                                print(ssc_init, d_amp)
                        elif useACE == False:
                            detection, det = True, True
                            if verbose == True:
                                print("Storm onset =", elevatedrange.ndarray[t_ind][0], d_amp)
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
                stream = remaininglst
            else:
                maxfound = False

        return detection, SSC_list

    # --------------------------------------------------------------------
    #       OPTIONAL FUNCTIONS:                                         #
    #               _calcDVals()                                        #
    #               _calcProbWithSat()                                  #
    # --------------------------------------------------------------------

    def _calcDVals(self, stream, key, m, n):
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

        array = [[] for key in DataStream().KEYLIST]
        headers = {}

        for i in range(1, len(y) - 1):
            # DEFINE ARRAYS ABOVE AND BELOW POINT:
            # ------------------------------------
            if i < m:
                s_up = n
                s_down = i
            elif i > (len(y) - n):
                s_up = len(y) - i
                s_down = m
            else:
                s_up = n
                s_down = m
            # Note: would be good to build in boundary behaviour here: mirrored, linear?

            y_lo = np.array(y[i - s_down:i], dtype=float64)
            y_lo = np.ma.masked_array(y_lo, np.isnan(y_lo))
            y_hi = np.array(y[i:i + s_up], dtype=float64)
            y_hi = np.ma.masked_array(y_hi, np.isnan(y_hi))

            # CALCULATE MEANS:
            # ----------------
            lo_mean, lo_std = np.mean(y_lo), np.std(y_lo)
            hi_mean, hi_std = np.mean(y_hi), np.std(y_hi)

            # ASSIGN VALUES WITH DIFFERENCES:
            # -------------------------------
            array[DataStream().KEYLIST.index('time')].append(t[i])
            array[DataStream().KEYLIST.index('x')].append(float(y[i]))
            array[DataStream().KEYLIST.index('dx')].append(float(hi_mean - lo_mean))
            array[DataStream().KEYLIST.index('var1')].append(float(lo_mean))
            array[DataStream().KEYLIST.index('var2')].append(float(hi_mean))
            array[DataStream().KEYLIST.index('var3')].append(float(lo_std))
            array[DataStream().KEYLIST.index('var4')].append(float(hi_std))

        headers['col-x'] = 'X'
        headers['col-dx'] = 'dX'

        for key in ['time', 'x', 'dx', 'var1', 'var2', 'var3', 'var4']:
            array[DataStream().KEYLIST.index(key)] = np.asarray(array[DataStream().KEYLIST.index(key)])

        return DataStream(header=headers, ndarray=np.asarray(array, dtype=object))

    def _calcProbWithSat(self, ssctime, sat_dict, dh_prob, dh_weight, satprob_weight, estt_weight, verbose=False):
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

        estssctime = sat_dict['estssctime']
        t_arr_low = estssctime - timedelta(minutes=self.ace_window)
        t_arr_high = estssctime + timedelta(minutes=self.ace_window)

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
            factor = (100. - base) / norm.pdf(0, mu, sigma)
            estt_prob = base + norm.pdf(diff, mu, sigma) * factor
            # if diff <= 10.: # minutes
            #    estt_prob = 100.
            # elif 10. < diff <= 20.:
            #    estt_prob = 75.
            # elif 20. < diff <= 30.:
            #    estt_prob = 50.
            estt_prob = estt_prob * estt_weight
            sat_prob = sat_dict['probf'] * satprob_weight
            total_weight = satprob_weight + dh_weight + estt_weight
            final_probf = (sat_prob + dh_prob + estt_prob) / total_weight

            # SSC_dict = {}
            # SSC_dict['ssctime'] = ssctime
            # SSC_dict['amp'] = d_amp
            # SSC_dict['duration'] = duration
            # SSC_dict['probf'] = final_probf
            return True, final_probf
        else:
            return False, None

    def createOutput(self, ssc_list, ACE_results=None, sensorid=None, aceid='ACE_swepam', operator='stormdet',
                     stationid=None, labelid='020', label='ssc geomagnetic storm'):
        '''
        DEFINITION:
            This function constructs a result dictionary corresponding to a flagging structure.

        PARAMETERS:
        Variables:
            - ssc_list:        list of dicts with ssc results
            - ACE_results:     list of dict with ACE results
            - sensorid:        SensorID of Magnetometer
            - acid:            Provisional ID for ACE
            - operator:        'stormdet' + method
            - stationid:       stationID of Magnetometer and L1 for ACE data
            - labelid          020
            - label            geomagnetic storm ssc

        RETURNS:
            dictionary of flagging 2.0 type
            # {0:{'sensorid':'LEMI','starttime':datetime.datetime(2024, 5, 10, 17, 1, 44),endtime:xxx,'flagtype':2,'group':['magnetism'],'stationid':'wic','labelid':'020','label':'ssc geomagnetic storm','amplitude':num,'probability':[num], 'operator':'stormdet-AIC'},"dummy2":{}}

        '''
        count = 0
        rd = {}
        for el in ssc_list:
            sscd = {}
            sscd['sensorid'] = sensorid
            sscd['starttime'] = el.get('ssctime')
            sscd['endtime'] = el.get('ssctime') + timedelta(seconds=el.get('duration'))
            sscd['flagtype'] = 2
            sscd['group'] = ['magnetism']
            sscd['stationid'] = stationid
            sscd['labelid'] = labelid
            sscd['label'] = label
            sscd['amplitude'] = el.get('amp')
            sscd['probability'] = el.get('probf')
            sscd['operator'] = operator
            rd[count] = sscd
            count += 1
        if ACE_results:
            for el in ACE_results:
                aced = {}
                aced['sensorid'] = aceid
                aced['starttime'] = el.get('satssctime')
                aced['endtime'] = el.get('estssctime')  # expected start time of SSC on Earth
                aced['flagtype'] = 2
                aced['group'] = ['solarwind']
                aced['stationid'] = 'L1'
                aced['labelid'] = labelid
                aced['label'] = label
                aced['comment'] = "solar wind speed={:.1f}, proton flux={:.1f}".format(el.get('vwind'), el.get('pflux'))
                aced['probability'] = el.get('probf')
                aced['operator'] = operator
                rd[count] = aced
                count += 1
        return rd


def seek_storm(magdata, satdata_1m=None, satdata_5m=None, method='AIC', variables=None,
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
       detection, ssc_list = seekStorm(magstream)
    '''

    # For testing purpose:
    from magpy.core import plot as mp

    stdt = StormDet()
    if not variables:
        variables = stdt.funcvars[method]

    detection = False
    ssc_list = []
    var_key = 'var1'

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
            ACE_detection, ACE_results = stdt.checkACE(satdata_1m, ACE_5m=satdata_5m, verbose=verbose)
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

    # day = datetime.strftime(num2date(magdata[10].time),'%Y-%m-%d')
    t_ind = magdata.KEYLIST.index('time')
    day = datetime.strftime(magdata.ndarray[t_ind][10], "%Y-%m-%d")

    a, p = variables[0], variables[1]

    # AIC - AKAIKE INFORMATION CRITERION
    # ----------------------------------
    if method == 'AIC':
        AIC_key = 'var2'
        # AIC_newkey = 'var4'
        AIC_dkey = 'var3'
        trange = 30

        magdata = magdata.aic_calc('x', timerange=timedelta(minutes=trange), aic2key=AIC_key, delete=True)
        magdata = magdata.derivative(keys=[AIC_key], put2keys=[AIC_dkey])

        minlen = variables[2]
        detection, ssc_list = stdt.findSSC_AIC(magdata, AIC_key, AIC_dkey, a, p, minlen,
                                               useACE=useACE, ACE_results=ACE_results, verbose=verbose)
        if plot_vars == True:
            mp.tsplot(magdata, ['x', AIC_key, AIC_dkey])

    # DWT - DISCRETE WAVELET TRANSFORM
    # --------------------------------
    elif method == 'DWT2' or method == 'DWT1':  # using D2 or D3 detail
        DWT = magdata.dwt_calc()
        if method == 'DWT2':
            var_key = 'var2'
        elif method == 'DWT1':
            var_key = 'var1'
        detection, ssc_list = stdt.findSSC(DWT, var_key, a, p, useACE=useACE, ACE_results=ACE_results, verbose=verbose)
        if plot_vars == True:
            mp.tsplot([magdata, DWT], [['x'], ['dx', 'var1', 'var2', 'var3']], title=day)

    # MODWT - MAXIMAL OVERLAP DISCRETE WAVELET TRANSFORM
    # --------------------------------------------------
    elif method == 'MODWT':  # using D2 or D3 detail
        MODWT = magdata.modwt_calc(level=1, wavelet='haar')
        var_key = 'var1'
        detection, ssc_list = stdt.findSSC(MODWT, var_key, a, p, useACE=useACE, ACE_results=ACE_results,
                                           verbose=verbose)
        if plot_vars == True:
            mp.tsplot([magdata, MODWT], [['x'], ['dx', 'var1']], title=day)

    # FDM - FIRST DERIVATION METHOD
    # -----------------------------
    elif method == 'FDM':
        FDM_key = 'dx'
        magdata = magdata.derivative(keys=['x'], put2keys=[FDM_key])
        magdata.multiply({FDM_key: 2}, square=True)
        detection, ssc_list = stdt.findSSC(magdata, FDM_key, a, p, useACE=useACE, ACE_results=ACE_results,
                                           verbose=verbose)
        if plot_vars == True:
            mp.tsplot(magdata, ['x', 'dx'])

    else:
        print("%s is an invalid evaluation function!" % method)

    # Convert ssc_list and ACE_results into a flagging 2.0 dictionary
    # -----------------------------
    result_dict = stdt.createOutput(ssc_list, ACE_results, sensorid=magdata.header.get('SensorID'), aceid='ACE_swepam',
                                    operator='stormdet-{}'.format(method), stationid=magdata.header.get('StationID'),
                                    labelid='020', label='ssc geomagnetic storm')

    return detection, result_dict


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
    import scipy
    # Creating a test data set of second resolution and 6 day length
    c = 2000  # 4000 nan values are filled at random places to get some significant data gaps
    array = [[] for el in DataStream().KEYLIST]
    win = scipy.signal.windows.hann(60)
    a = np.random.uniform(20950, 21000, size=122800)
    b = np.random.uniform(20950, 21050, size=138400)
    x = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
    x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
    array[1] = x[1000:-1000]
    a = np.random.uniform(1950, 2000, size=122800)
    b = np.random.uniform(1900, 2050, size=138400)
    y = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
    y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
    array[2] = y[1000:-1000]
    a = np.random.uniform(44300, 44400, size=261200)
    z = scipy.signal.convolve(a, win, mode='same') / sum(win)
    array[3] = z[1000:-1000]
    array[0] = np.asarray([datetime(2022, 11, 21) + timedelta(seconds=i) for i in range(0, len(array[1]))])
    teststream = DataStream([], {'SensorID': 'Test_0001_0001'}, np.asarray(array, dtype=object))

    errors = {}
    successes = {}
    testrun = 'streamtestfile'
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)
    while True:
        try:
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            minstream = teststream.filter()
            k = K_fmi(minstream, debug=False)
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            successes['K_fmi'] = (
                "Version: {}, K_fmi: {}".format(magpyversion, (te - ts).total_seconds()))
            print ('Mean K', k.mean('var1'))
        except Exception as excep:
            errors['K_fmi'] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR determining K_fmi.")
        if emdpackage:
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                minstream = teststream.filter()
                bs = sqbase(minstream, components=['z'], baseline_type='')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['qdbaseline'] = (
                    "Version: {}, qdbaseline: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['qdbaseline'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR determining quiet day baseline")
        try:
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            teststream = teststream.filter(missingdata='interpolate', noresample=True)
            for method in ['AIC','DWT2','MODWT','FDM']:
                k = seek_storm(teststream.trim(starttime='2022-11-22',endtime='2022-11-23'), satdata_1m=None, satdata_5m=None, verbose=False, method=method)
                print ("Successfully tested {} for storm detection".format(method))
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            successes['seek_storm'] = (
                "Version: {}, seek_storm: {}".format(magpyversion, (te - ts).total_seconds()))
        except Exception as excep:
            errors['seek_storm'] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with seek_storm.")


        break

    t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
    time_taken = t_end_test - t_start_test
    print(datetime.now(timezone.utc).replace(tzinfo=None), "- Stream testing completed in {} s. Results below.".format(time_taken.total_seconds()))

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
