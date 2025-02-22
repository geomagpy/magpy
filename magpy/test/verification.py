import unittest
import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2
from magpy.stream import *
import magpy.absolutes as di
from magpy.core.flagging import *
from magpy.core import database
from magpy.core import activity
from magpy.core import plot as mp
import scipy

def create_verificationstream(startdate=datetime(2022, 11, 22)):
    teststream = DataStream()
    array = [[] for el in DataStream().KEYLIST]
    array[1] = [20000] * 720
    array[1].extend([22000] * 720)
    array[1] = np.asarray(array[1])
    array[2] = np.asarray([0] * 1440)
    array[3] = np.asarray([20000] * 1440)
    array[6] = np.asarray([np.nan] * 1440)
    # array[4] = np.sqrt((x*x) + (y*y) + (z*z))
    array[0] = np.asarray([startdate + timedelta(minutes=i) for i in range(0, len(array[1]))])
    rain1 = np.arange(1,721,1.0)
    rain2 = np.arange(1,361,0.5)
    rain = np.concatenate((rain1, rain2))
    array[7] = rain
    array[KEYLIST.index('sectime')] = np.asarray(
        [startdate + timedelta(minutes=i) for i in range(0, len(array[1]))]) + timedelta(minutes=15)
    teststream = DataStream(header={'SensorID': 'Test_0002_0001'}, ndarray=np.asarray(array, dtype=object))
    teststream.header['col-x'] = 'X'
    teststream.header['col-y'] = 'Y'
    teststream.header['col-z'] = 'Z'
    teststream.header['col-t2'] = 'Text'
    teststream.header['col-var1'] = 'Rain'
    teststream.header['unit-col-x'] = 'nT'
    teststream.header['unit-col-y'] = 'nT'
    teststream.header['unit-col-z'] = 'nT'
    teststream.header['unit-col-t2'] = 'degC'
    teststream.header['unit-col-var1'] = 'mm'
    teststream.header['DataComponents'] = 'XYZ'
    return teststream

def create_activityverificationstream(startdate=datetime(2022, 11, 22), resolution='seconds'):
    teststream = DataStream()
    # Creating a test data set of second resolution and 3 day length
    c = 2000  # 4000 nan values are filled at random places to get some significant data gaps
    array = [[] for el in DataStream().KEYLIST]
    array[1] = [20000]*121800 # 259200
    array[1].extend([22000]*137400)
    win = scipy.signal.windows.hann(60)
    a = np.random.uniform(1950, 2000, size=122800)
    b = np.random.uniform(1900, 2050, size=138400)
    y = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
    y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
    array[2] = y[1000:-1000]
    a = np.random.uniform(44300, 44400, size=261200)
    z = scipy.signal.convolve(a, win, mode='same') / sum(win)
    array[3] = z[1000:-1000]
    if resolution=='seconds':
        array[0] = np.asarray([datetime(2022, 11, 21) + timedelta(seconds=i) for i in range(0, len(array[1]))])
    else:
        array[0] = np.asarray([datetime(2022, 11, 21) + timedelta(minutes=i) for i in range(0, len(array[1]))])
    teststream = DataStream([], {'SensorID': 'Test_0001_0001'}, np.asarray(array, dtype=object))
    teststream = DataStream(header={'SensorID': 'Test_0002_0001'}, ndarray=np.asarray(array, dtype=object))
    teststream.header['col-x'] = 'X'
    teststream.header['col-y'] = 'Y'
    teststream.header['col-z'] = 'Z'
    teststream.header['unit-col-x'] = 'nT'
    teststream.header['unit-col-y'] = 'nT'
    teststream.header['unit-col-z'] = 'nT'
    teststream.header['DataComponents'] = 'XYZ'
    return teststream

teststream = create_verificationstream()


class TestStream(unittest.TestCase):

    def test_aic(self):
        # tested as part of seek storm in activity module
        pass

    def test_convertstream(self):
        # also tests idf and hdz tools
        conv = teststream.copy()
        idf = conv.xyz2idf()
        meand1 = idf.mean('y')
        xyz = idf.idf2xyz()
        hdz = xyz.xyz2hdz()
        meand2 = hdz.mean('y')
        xyz = hdz.hdz2xyz()
        # test for floating point accuracy
        val1 = [np.round(teststream.ndarray[1][0], 8), np.round(teststream.ndarray[2][0], 8),
                np.round(teststream.ndarray[3][0], 8)]
        val2 = [np.round(xyz.ndarray[1][0], 8), np.round(xyz.ndarray[2][0], 8), np.round(xyz.ndarray[3][0], 8)]
        self.assertEqual(meand1, meand2)
        self.assertEqual(teststream.length(), xyz.length())
        self.assertEqual(val1, val2)

    def test_copy_column(self):
        orgstream = teststream.copy()
        self.assertEqual(orgstream.length(), teststream.length())

    def test_det_trange(self):
        # see Jankowski and Sucksdorff (1996), page 140
        self.assertEqual(np.round(teststream._det_trange(120.) * (3600. * 24.), 4), 48.2561)

    def test_drop_column(self):
        cpstream = teststream.copy()
        dropstream = cpstream._drop_column('z')
        self.assertEqual(len(dropstream.ndarray[3]), 0)

    def test_get_column(self):
        col = teststream._get_column('z')
        self.assertEqual(np.mean(col), 20000)

    def test_get_key_headers(self):
        keys = teststream._get_key_headers()
        self.assertEqual(keys, ['x', 'y', 'z', 't2', 'var1', 'sectime'])

    def test_get_max(self):
        max = teststream._get_max('x')
        self.assertEqual(max, 22000)

    def test_get_min(self):
        min = teststream._get_min('x')
        self.assertEqual(min, 20000)

    def test_get_variance(self):
        var = teststream._get_variance('x')
        self.assertEqual(var, 1000000)

    def test_move_column(self):
        cpstream = teststream.copy()
        mvstream = cpstream._move_column('x', 'z')
        self.assertEqual(21000, np.mean(mvstream.ndarray[3]))

    def test_put_column(self):
        cpstream = teststream.copy()
        col = cpstream._get_column('x')
        pustream = cpstream._put_column(col, 'var1')
        self.assertEqual(len(pustream._get_column('var1')), 1440)

    def test_remove_nancolumns(self):
        before = teststream._get_key_headers()
        nanstream = teststream._remove_nancolumns()
        after = nanstream._get_key_headers()
        val = [x for x in before if not x in after]
        self.assertEqual(val, ['t2'])

    def test_select_keys(self):
        xystream = teststream._select_keys(keys=['x', 'y'])
        self.assertEqual(len(xystream.ndarray[1]), 1440)
        self.assertEqual(len(xystream.ndarray[2]), 1440)
        self.assertEqual(len(xystream.ndarray[3]), 0)

    def test_select_timerange(self):
        ar = teststream._select_timerange(starttime='2022-11-22T08:00:00', endtime='2022-11-22T09:00:00')
        self.assertEqual(len(ar[0]), 60)

    def test_tau(self):
        # see Jankowski and Sucksdorff (1996), page 140
        self.assertAlmostEqual(teststream._tau(120.) * (3600. * 24.), 15.90062182)

    def test_aic_calc(self):
        # tested as part of seek storm in activity module
        pass

    def test_amplitude(self):
        amp = teststream.amplitude('x')
        self.assertEqual(amp, 2000)

    def test_apply_deltas(self):
        fstream = teststream.calc_f()
        # old type
        # print("apply_deltas: testing with old database input type")
        ddv1 = "st_690.0,f_-1.48,time_timedelta(seconds=-3.0),et_17532.0;st_17532.0,f_-1.571,time_timedelta(seconds=-3.0),et_17788.5;st_17788.5,f_-1.571,time_timedelta(seconds=1.50),et_17897.0;st_17897.0,f_-1.631,time_timedelta(seconds=-0.30),et_18262.0;st_18262.0,f_-1.616,time_timedelta(seconds=-0.28),et_18628.0;st_18628.0,f_-1.609,time_timedelta(seconds=-0.28),et_18993.0;st_18993.0,f_-1.655,time_timedelta(seconds=-0.33),et_19358.0;st_19358.0,f_-1.729,time_timedelta(seconds=-0.28)"
        fstream.header["DataDeltaValues"] = ddv1
        res1 = fstream.apply_deltas()
        diff1 = fstream.mean('f') - res1.mean('f')
        self.assertEqual(np.round(diff1, 3), 1.655)
        # new type
        # print("apply_deltas: testing with new database input type")
        ddv2 = '{"0": {"st": "1971-11-22 00:00:00", "f": -1.48, "time": "timedelta(seconds=-3.0)", "et": "2018-01-01 00:00:00"}, "1": {"st": "2018-01-01 00:00:00", "f": -1.571, "time": "timedelta(seconds=-3.0)", "et": "2018-09-14 12:00:00"}, "2": {"st": "2018-09-14 12:00:00", "f": -1.571, "time": "timedelta(seconds=1.50)", "et": "2019-01-01 00:00:00"}, "3": {"st": "2019-01-01 00:00:00", "f": -1.631, "time": "timedelta(seconds=-0.30)", "et": "2020-01-01 00:00:00"}, "4": {"st": "2020-01-01 00:00:00", "f": -1.616, "time": "timedelta(seconds=-0.28)", "et": "2021-01-01 00:00:00"}, "5": {"st": "2021-01-01 00:00:00", "f": -1.609, "time": "timedelta(seconds=-0.28)", "et": "2022-01-01 00:00:00"}, "6": {"st": "2022-01-01 00:00:00", "f": -1.655, "time": "timedelta(seconds=-0.33)", "et": "2023-01-01 00:00:00"}, "7": {"st": "2023-01-01 00:00:00", "f": -1.729, "time": "timedelta(seconds=-0.28)"}}'
        fstream.header["DataDeltaValues"] = ddv2
        res2 = fstream.apply_deltas()
        diff2 = fstream.mean('f') - res2.mean('f')
        self.assertEqual(np.round(diff2, 3), 1.655)

    def test_baseline(self):
        variodata = read(example1)
        # Test part - simplify teststream by just using constant mean value of variation for this day
        zmean = variodata.mean('z')
        variodata._put_column([variodata.mean('x')] * len(variodata), 'x')
        variodata._put_column([variodata.mean('y')] * len(variodata), 'y')
        variodata._put_column([zmean] * len(variodata), 'z')
        # Load basevalues and apply
        basevalues = read(example3)
        func = variodata.baseline(basevalues)
        # Correct data set with basevalues
        corrdata = variodata.bc()
        zcorr = corrdata.mean('z')
        # Now get the original base values for z
        #  - just a rough estimate for testing - not based on the correct spline fitted basevalues
        tb = basevalues.mean('dz')
        self.assertEqual(np.round(zmean + tb, 1), np.round(zcorr, 1))

    def test_bc(self):
        # Tested in test_baseline
        pass

    def test_calc_f(self):
        fstream = teststream.calc_f()
        self.assertEqual(len(fstream.ndarray[4]), 1440)
        fval = fstream.ndarray[4][0]
        self.assertEqual(fval, np.sqrt(20000 * 20000 + 20000 * 20000))

    def test_contents(self):
        d = teststream.contents()
        cont = d.get('x')
        colname  = cont.get('columnname')
        self.assertEqual(colname, 'X')

    def test_compensation(self):
        teststream.header['DataCompensationX'] = -10
        teststream.header['DataCompensationY'] = 0
        teststream.header['DataCompensationZ'] = 10
        compstream = teststream.compensation()
        minx = compstream._get_min('x')
        minz = compstream._get_min('z')
        self.assertEqual(minx, 30000)
        self.assertEqual(minz, 10000)

    def test_cut(self):
        cutstream = teststream.cut(50, kind=0, order=0)
        self.assertEqual(len(cutstream), 720)
        self.assertEqual(cutstream.start(), datetime(2022, 11, 22, 12, 0))
        cutstream = teststream.cut(10, kind=1, order=1)
        self.assertEqual(len(cutstream), 10)
        self.assertEqual(cutstream.start(), datetime(2022, 11, 22, 0, 0))

    def test_dailymeans(self):
        dmt = teststream.dailymeans()
        x = dmt._get_column('x')
        self.assertEqual(len(x), 1)
        self.assertEqual(x[0], 21000.)

    def test_delta_f(self):
        fstream = teststream.calc_f()
        dfstream = fstream.delta_f()
        mindf = dfstream._get_min('df')
        maxdf = dfstream._get_min('df')
        self.assertEqual(mindf, maxdf)
        self.assertEqual(mindf, 0.0)

    def test_determine_rotationangles(self):
        # see test_rotation
        pass

    def test_dict2stream(self):
        # part of baseline/bc test
        pass

    def test_derivative(self):
        diffdata = teststream.derivative(keys=['x', 'y', 'z'], put2keys=['dx', 'dy', 'dz'])
        self.assertNotEqual(np.mean(diffdata.ndarray[12][719:720]), 0)
        self.assertEqual(np.mean(diffdata.ndarray[12][717:718]), 0)

    def test_dwt_calc(self):
        # tested as part of seek storm in activity module
        pass

    def test_end(self):
        end = teststream.end()
        self.assertEqual(end, datetime(2022, 11, 22, 23, 59))

    def test_extend(self):
        a = teststream.cut(25)
        b = teststream.cut(25, order=1)
        a.extend([], b.header, b.ndarray)
        self.assertEqual(len(a), 720)

    def test_extract(self):
        extstream = teststream.extract("x", 20000, ">")
        self.assertEqual(len(extstream), 720)

    def test_extrapolate(self):
        t1 = teststream.trim(starttime='2022-11-22T09:00:00', endtime='2022-11-22T14:00:00')
        ex1 = t1.extrapolate(starttime='2022-11-22T07:00:00', endtime='2022-11-22T16:00:00', method='spline')
        self.assertEqual(len(ex1), 538)

    def test_filter(self):
        filtereddata = teststream.filter()
        filtereddata = filtereddata.trim(starttime='2022-11-22T11:56:00', endtime='2022-11-22T12:04:00')
        function = filtereddata.interpol(['x'])
        valx = function[0]['fx']([0.5])
        valx = function[0]['fx']([0.5])
        self.assertEqual(np.round(valx, 5), 21000.)

    def test_findtime(self):
        index = teststream.findtime("2022-11-22T12:00:00")
        self.assertEqual(index, 720)

    def test_fit(self):
        teststream = create_verificationstream()
        trimstream = teststream.trim(starttime="2022-11-22T09:00:00", endtime="2022-11-22T14:00:00")
        orgmean = trimstream.mean('x')
        for fitoption in ['poly', 'harmonic', 'least-squares', 'mean', 'spline']:
            print(" Testing fitting with {} method".format(fitoption))
            func = trimstream.fit(['x'], fitfunc=fitoption, fitdegree=5, knotstep=0.2)
            # mp.tsplot([trimstream], keys=[['x']], functions=[[func]])
            funcstream = trimstream.copy()
            funcstream = funcstream.func2stream(func, keys=['x'], mode='values')
            # mp.tsplot([funcstream], keys=[['x']])
            self.assertAlmostEqual(orgmean, funcstream.mean('x'))

    def test_func2header(self):
        teststream = create_verificationstream()
        func = teststream.interpol(['x'])
        teststream = teststream.func2header(func)
        f = teststream.header.get('DataFunctionObject')
        self.assertIsInstance(f, (list, tuple))

    def test_func2stream(self):
        teststream = create_verificationstream()
        teststream = teststream.remove(starttime='2022-11-22T07:00:00', endtime='2022-11-22T08:00:00')
        teststream = teststream.get_gaps()
        contfunc = teststream.interpol(['x', 'y'])
        teststream = teststream.func2stream(contfunc, keys=['x', 'y'], mode='values')
        ind = teststream.findtime('2022-11-22T07:30:00')
        xval = teststream.ndarray[1][ind]
        zval = teststream.ndarray[3][ind]
        nantest = False
        if np.isnan(zval):
            nantest = True
        self.assertEqual(xval, 20000.)
        self.assertTrue(nantest)

    def test_get_fmi_array(self):
        # tested as part of k_fmi in activity module
        pass

    def test_get_gaps(self):
        gapstream = teststream.remove(starttime='2022-11-22T08:00:00', endtime='2022-11-22T09:00:00')
        self.assertEqual(len(teststream), len(gapstream) + 61)
        fullstream = gapstream.get_gaps()
        self.assertEqual(len(teststream), len(fullstream))

    def test_get_key_name(self):
        kn = teststream.get_key_name('x')
        self.assertEqual(kn, 'X')

    def test_get_key_unit(self):
        ku = teststream.get_key_unit('x')
        self.assertEqual(ku, 'nT')

    def test_get_sampling_period(self):
        sr = teststream.get_sampling_period()
        self.assertEqual(np.round(sr, 1), 60.0)

    def test_harmfit(self):
        # tested in _test_fit with harmonic fit
        pass

    def test_integrate(self):
        # Not yet tested - only graphically
        # Why? because this method is not really used so far
        pass

    def test_interpol(self):
        # tested in smooth and filter method
        pass

    def test_interpolate_nans(self):
        # get column y
        y = teststream._get_column('y')
        # put 100 nan values at random positions
        y.ravel()[np.random.choice(y.size, 100, replace=False)] = np.nan
        # count nans
        n_nan = np.count_nonzero(np.isnan(y))
        self.assertEqual(n_nan, 100)
        stream2inter = teststream._put_column(y, 'y')
        getback = stream2inter.interpolate_nans(['y'])
        y = getback._get_column('y')
        n_nan_now = np.count_nonzero(np.isnan(y))
        self.assertEqual(n_nan_now, 0)

    def test_length(self):
        # Tests also _get_sampling_period
        self.assertEqual(teststream.length()[0], 1440)
        self.assertEqual(len(teststream), 1440)

    def test_mean(self):
        pmeanx = teststream.mean('x')
        self.assertEqual(pmeanx, 21000)

    def test_modwt_calc(self):
        # tested as part of seek storm in activity module
        pass

    def test_multiply(self):
        mstream = teststream.multiply({'x': 2})
        pmeanx = mstream.mean('x')
        self.assertEqual(pmeanx, 42000)

    def test_offset(self):
        ostream = teststream.copy()
        ostream = ostream.offset({'x': 10}, starttime='2022-11-22T00:00:00', endtime='2022-11-22T12:00:00')
        diff = ostream.mean('x') - teststream.mean('x')
        self.assertEqual(np.round(diff, 1), 5)
        self.assertEqual(ostream.ndarray[1][0], teststream.ndarray[1][0] + 10)
        ostream = ostream.offset({'time': 'timedelta(seconds=-0.5)'}, starttime='2022-01-01', endtime='2023-01-01')
        st, et = ostream.timerange()
        self.assertEqual(st, testtime('2022-11-21T23:59:59.5'))

    def test_randomdrop(self):
        dropstream = teststream.randomdrop(percentage=50, fixed_indicies=[0, len(teststream) - 1])
        self.assertEqual(len(dropstream), 720)
        self.assertEqual(dropstream.end(), datetime(2022, 11, 22, 23, 59))

    def test_remove(self):
        dstream = teststream.remove(starttime='2022-11-22T08:00:00', endtime='2022-11-22T09:00:00')
        self.assertEqual(len(teststream), len(dstream) + 61)

    def test_resample(self):
        resampleddata = teststream.resample(['x', 'y', 'z'], period=120, debugmode=True)
        # x has a jump between two points. check diff whether there is any time shift
        diff = subtract_streams(teststream, resampleddata)
        self.assertEqual(diff.mean('x'), 0.0)
        self.assertEqual(diff.mean('y'), 0.0)
        self.assertEqual(diff.mean('z'), 0.0)
        self.assertEqual(len(teststream), len(resampleddata) * 2)

    def test_rotation(self):
        cutstream = teststream.cut(50, kind=0, order=1)
        pmeanx = cutstream.mean('x')
        pmeany = cutstream.mean('y')
        pmeanz = cutstream.mean('z')
        print (pmeanx, pmeanz)
        cpstream = cutstream.copy()
        # testing horizontal rotation
        protstream = cutstream.rotation(alpha=45)
        smeanx = protstream.mean('x')
        smeany = protstream.mean('y')
        print (smeanx, smeany, np.sqrt(smeanx**2 + smeany**2))
        self.assertEqual(np.round(np.sqrt(smeanx ** 2 + smeany ** 2), 5), np.round(np.abs(pmeanx), 5))
        # testing complex rotation
        srotstream = cutstream.rotation(alpha=45, beta=45)
        tmeanx = srotstream.mean('x')
        tmeany = srotstream.mean('y')
        tmeanz = srotstream.mean('z')
        print (tmeanx,tmeany,tmeanz)
        self.assertEqual(np.round(tmeanx, 5), np.round(pmeanx, 5))
        self.assertEqual(np.round(tmeany, 5), np.round(pmeanz, 5))
        self.assertEqual(np.round(tmeanz, 5), 0)
        # testing complex rotation
        rotstream = cutstream.rotation(alpha=45, beta=135)
        meanx = rotstream.mean('x')
        meany = rotstream.mean('y')
        meanz = rotstream.mean('z')
        print (meanx,meany,meanz)
        self.assertEqual(np.round(np.sqrt(pmeanx ** 2 + pmeanz ** 2), 5), -1 * np.round(meanz, 5))
        # testing inversion of complex rotation
        orgstream = rotstream.rotation(alpha=45, beta=135, invert=True)
        omeanx = orgstream.mean('x')
        omeany = orgstream.mean('y')
        omeanz = orgstream.mean('z')
        print (omeanx,omeany,omeanz)
        self.assertEqual(np.round(omeanx, 5), np.round(pmeanx, 5))
        self.assertEqual(np.round(omeany, 5), np.round(pmeany, 5))
        self.assertEqual(np.round(omeanz, 5), np.round(pmeanz, 5))
        alpha, beta, gamma = srotstream.determine_rotationangles(referenceD=0.0, referenceI=45.0)
        self.assertEqual(np.round(alpha, 5), -45)
        self.assertEqual(np.round(beta, 5), -45)

    def test_samplingrate(self):
        # Tests also _get_sampling_period
        self.assertEqual(teststream.samplingrate(), 60)

    def test_simplebasevalue2stream(self):
        # Tested in test_absolute_analysis
        pass

    def test_smooth(self):
        teststream = create_verificationstream()
        teststream = teststream._remove_nancolumns()
        smootheddata = teststream.smooth(window='hanning', window_len=11)
        smootheddata = smootheddata.trim(starttime='2022-11-22T11:56:00', endtime='2022-11-22T12:04:00')
        function = smootheddata.interpol(['x'])
        valx = function[0]['fx']([0.5])
        self.assertEqual(np.round(valx, 5), 21000.)

    def test_sorting(self):
        # skipping this test for now
        pass

    def test_start(self):
        start = teststream.start()
        self.assertEqual(start, datetime(2022, 11, 22))

    def test_stats(self):
        d = teststream.stats()
        le = d.get("Amount")
        self.assertEqual(le, len(teststream))

    def test_steadyrise(self):
        # To be done with rain analysis
        teststream = create_verificationstream()
        col = teststream.steadyrise('var1', timedelta(minutes=60), sensitivitylevel=0.002)
        self.assertEqual(np.floor(np.mean(col)), 44.0)

    def test_stream2dict(self):
        # part of baseline/bc test
        pass

    def test_trim(self):
        t1 = teststream.trim(starttime='2022-11-22T09:00:00', endtime='2022-11-22T14:00:00')
        self.assertEqual(len(t1), 300)

    def test_timerange(self):
        t1, t2 = teststream.timerange()
        self.assertEqual(t1, datetime(2022, 11, 22))
        self.assertEqual(t2, testtime("2022-11-22T23:59:00"))

    def test_union(self):
        uniq = teststream.union(np.asarray([1, 1, 2, 2, 3, 3, 3, 3, 3]))
        self.assertEqual(uniq, [1,2,3])

    def test_unique(self):
        uniq = teststream.unique('time')
        self.assertEqual(len(uniq),len(teststream))

    def test_use_sectime(self):
        tcolumn = teststream._get_column('time')
        newtcolumn = np.asarray([element + timedelta(minutes=15) for element in tcolumn])
        teststream._put_column(newtcolumn, 'sectime')
        shifted_teststream = teststream.use_sectime()
        st = shifted_teststream.start()
        self.assertEqual(st, datetime(2022, 11, 22, 0, 15))

    def test_variables(self):
        v = teststream.variables()
        self.assertEqual(v, ['x', 'y', 'z', 't2', 'var1', 'sectime'])

    def test_write(self):
        # Tested in the format libraries
        # and in runtime tests with all libraries
        pass

    def test_func_to_from_file(self):
        teststream = create_verificationstream()
        keys = ['x', 'y']
        contfunc = teststream.interpol(keys)
        func_to_file(contfunc, "/tmp/savedparameter.json")
        funcparameter = func_from_file("/tmp/savedparameter.json")
        firstf = funcparameter.get('0')
        self.assertEqual(firstf.get('keys'), keys)
        self.assertEqual(firstf.get('fitfunc'), 'linear')


class TestMethods(unittest.TestCase):

    def test_ceil_dt(self):
        t1 = ceil_dt(datetime(2014,1,1,14,12,4), 60)
        t2 = ceil_dt(datetime(2014,1,1,14,12,4), 3600)
        t1ver = datetime(2014,1,1,14,13)
        t2ver = datetime(2014,1,1,15,00)
        self.assertEqual(t1, t1ver)
        self.assertEqual(t2, t2ver)

    def test_convert_geo_coordinate(self):
        lon, lat = convert_geo_coordinate(-34833.41399,310086.6051,'epsg:31256','epsg:4326')
        self.assertEqual(np.round(lon,3), 15.866)
        self.assertEqual(np.round(lat,3), 47.928)

    def test_data_for_di(self):
        # main test in absolutes
        data = data_for_di(example5, starttime="2018-08-29", datatype='both')
        self.assertEqual(len(data), 86400)

    def test_dates_to_url(self):
        url = "https://example.com?getdata"
        newurl = dates_to_url(url, starttime=datetime(2016,1,1), endtime=datetime(2016,1,4), starttimestring='starttime', endtimestring='endtime')
        self.assertEqual(len(newurl),87)

    def test_dictdiff(self):
        da = {0:"a",1:"b",2:"c"}
        db = {0:"a",1:"b",2:"d"}
        res = dictdiff(da, db, show_value_diff=True)
        ver = { 2 : ('c', 'd')}
        self.assertDictEqual(res.get('value_diffs'), ver)

    def test_dictgetlast_dict2string_string2dict(self):
        testtxt = 'A2_(2017_(deltaD_0.00;deltaI_0.201;deltaF_1.12);2018_(deltaF_1.11))'
        d1 = string2dict(testtxt)
        d2 = string2dict('st_736677.0,time_timedelta(seconds=-2.3),et_736846.0', typ='listofdict')
        result = dicgetlast(d1,pier='A2',element='deltaD,deltaI,deltaF')
        self.assertEqual(float(result.get('deltaF')), 1.11)
        t1 = dict2string(d1)
        t2 = dict2string(d2, typ='listofdict')
        self.assertEqual(t1, testtxt)

    def test_extract_date_from_string(self):
        datestr = ['gddtw_2022-11-22.txt', 'gddtw_20221122.txt','2022-11-22T12:00:00_data.txt']
        daterange = '2022_data.txt'
        ref = datetime.date(datetime(2022,11,22))
        for dat in datestr:
            d = extract_date_from_string(dat)
            self.assertEqual(ref, d[0])
        d = extract_date_from_string(daterange)
        self.assertTrue(d[0] <= ref <= d[1])

    def test_find_nearest(self):
        ar = [1,2,3,4,5,6]
        arvalue, aridx = find_nearest(ar,3.2)
        self.assertEqual(arvalue, 3)
        self.assertEqual(aridx, 2)

    def test_find_nth(self):
        i = find_nth("Hello_World_I_am_here","_", 3)
        self.assertEqual(i, 13)

    def test_get_chunks(self):
        l = get_chunks(10800, wl=3600)
        self.assertEqual(len(l), 2)

    def test_group_indices(self):
        indlist = [0,2,3,2000,2005,2006,2007,2008,2034,2037,2040,2041,2042,2050]
        group = group_indices(indlist)
        self.assertEqual(group,[[0, 0], [3, 3], [8, 8], [9, 9], [13, 13], [1, 2], [4, 7], [10, 12]])

    def test_is_number(self):
        self.assertFalse(is_number("drt345"))
        self.assertTrue(is_number("345"))

    def test_mask_nan(self):
        v = np.array([1, 2, 3, 4, 5, np.nan, 7, 8, 9, 10, 11, 12, 13, 14])
        a = maskNAN(v)
        self.assertTrue(a.mask[5])

    def test_missingvalue(self):
        v = np.array([1, 2, 3, 4, 5, np.nan, 7, 8, 9, 10, 11, 12, 13, 14])
        mv = missingvalue(v, window_len=10, fill='interpolate', fillvalue=99)
        self.assertEqual(6, mv[5])
        mv = missingvalue(v, window_len=10, fill='value', fillvalue=99)
        self.assertEqual(99, mv[5])

    def test_nan_helper(self):
        v = np.array([1, 2, 3, 4, 5, np.nan, 7, 8, 9, 10, 11, 12, 13, 14])
        a, b = nan_helper(v)
        self.assertTrue(a[5])

    def test_nearestpow2(self):
        res = nearestPow2(15)
        self.assertEqual(res, 16)

    def test_normalize(self):
        il = [1,2,3,4,6,5,7,8]
        e = normalize(np.asarray(il))
        self.assertEqual(e[0][-1], 1)

    def test_round_second(self):
        t1 = datetime(2022,11,22,11,27,13,654321)
        t2 = datetime(2022,11,22,11,27,13,254321)
        t3 = datetime(2022,11,22,23,59,59,654321)
        self.assertEqual(round_second(t1), datetime(2022,11,22,11,27,14))
        self.assertEqual(round_second(t2), datetime(2022,11,22,11,27,13))
        self.assertEqual(round_second(t3), datetime(2022,11,23))

    def test_test_timestring(self):
        self.assertEqual(test_timestring("2022-11-22"), datetime(2022,11,22))


class TestFlagging(unittest.TestCase):
    def test_add(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
        self.assertEqual(len(fl), 2)

    def test_list(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
        l = fl._list(['starttime', 'endtime'])
        self.assertEqual(len(l), len(fl))
        self.assertEqual(l[0][1], datetime(2022, 11, 22, 23, 56, 12, 654362))

    def test_copy(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
        newfl = fl.copy()
        newfl = newfl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T19:56:12.654362",
                          endtime="2022-11-22T19:59:12.654362", components=['x', 'y', 'z'], debug=False)
        self.assertNotEqual(newfl, fl)
        self.assertEqual(len(fl), 2)
        self.assertEqual(len(newfl), 3)

    def test_trim(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
        newfl = fl.copy()
        newfl = newfl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T19:56:12.654362",
                          endtime="2022-11-22T19:59:12.654362", components=['x', 'y', 'z'], debug=False)
        test = newfl.trim(starttime='2022-11-22T19:57:12.654362', endtime='2022-11-22T22:59:12.654362')
        self.assertEqual(len(test), 2)
        test = newfl.trim(starttime='2022-11-22T20:53:12.654362', endtime='2022-11-22T20:59:12.654362')
        self.assertEqual(len(test), 0)

    def test_select(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
        newfl = fl.copy()
        newfl = newfl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T19:56:12.654362",
                          endtime="2022-11-22T19:59:12.654362", components=['x', 'y', 'z'], debug=False)
        newfl = newfl.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T10:56:12.654362",
                          endtime="2022-11-22T10:59:12.654362", components=['f'], labelid='050', debug=False)
        newfl = newfl.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:56:12.654362",
                          endtime="2022-11-22T09:59:12.654362", components=['f'], labelid='001',
                          comment="incredible lightning strike", debug=False)
        obt1 = newfl.select('labelid', ['050'])
        obt2 = newfl.select('comment', ['lightning'])
        self.assertEqual(len(obt1), 1)
        self.assertEqual(len(obt2), 1)
        self.assertNotEqual(len(newfl), len(obt1))

    def test_join(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
        fo = Flags()
        fo = fo.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T10:56:12.654362",
                    endtime="2022-11-22T10:59:12.654362", components=['f'], labelid='050', debug=False)
        fo = fo.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:56:12.654362",
                    endtime="2022-11-22T09:59:12.654362", components=['f'], labelid='001',
                    comment="incredible lightning strike", debug=False)
        comb = fl.join(fo)
        self.assertEqual(len(comb), 4)

    def test_stats(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
        fo = Flags()
        fo = fo.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T10:56:12.654362",
                    endtime="2022-11-22T10:59:12.654362", components=['f'], labelid='050', debug=False)
        fo = fo.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:56:12.654362",
                    endtime="2022-11-22T09:59:12.654362", components=['f'], labelid='001',
                    comment="incredible lightning strike", debug=False)
        out = fo.stats(intensive=True, output='variable')
        self.assertIs(type(out), str)

    def test_diff(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T23:56:12.654362",
                    endtime="2022-11-22T23:59:12.654362", components=['x', 'y', 'z'], debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T21:56:12.654362",
                    endtime="2022-11-22T21:59:12.654362", components=['x', 'y', 'z'], debug=False)
        fl = fl.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:56:12.654362",
                    endtime="2022-11-22T09:59:12.654362", components=['f'], labelid='001',
                    comment="incredible lightning strike", debug=False)
        fo = Flags()
        fo = fo.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T10:56:12.654362",
                    endtime="2022-11-22T10:59:12.654362", components=['f'], labelid='050', debug=False)
        fo = fo.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:56:12.654362",
                    endtime="2022-11-22T09:59:12.654362", components=['f'], labelid='001',
                    comment="incredible lightning strike", debug=False)
        diff = fl.diff(fo)
        self.assertEqual(len(diff), 1)

    def test_drop(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T23:56:12.654362",
                    endtime="2022-11-22T23:59:12.654362", components=['x', 'y', 'z'], debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T21:56:12.654362",
                    endtime="2022-11-22T21:59:12.654362", components=['x', 'y', 'z'], debug=False)
        fl = fl.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:56:12.654362",
                    endtime="2022-11-22T09:59:12.654362", components=['f'], labelid='001',
                    comment="incredible lightning strike", debug=False)
        newfl = fl.drop(parameter='sensorid', values=['GSM90_Y1112_0001'])
        self.assertEqual(len(newfl), 2)

    def test_replace(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T23:56:12.654362",
                    endtime="2022-11-22T23:59:12.654362", components=['x', 'y', 'z'], debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T21:56:12.654362",
                    endtime="2022-11-22T21:59:12.654362", components=['x', 'y', 'z'], debug=False)
        fl = fl.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:56:12.654362",
                    endtime="2022-11-22T09:59:12.654362", components=['f'], labelid='001',
                    comment="incredible lightning strike", debug=False)
        flagsmodified = fl.replace('comment', 'lightning', 'hell of a lightining strike')
        flagsmodified = flagsmodified.replace('groups', None, ['magnetism'])
        flagsmodified = flagsmodified.replace('stationid', '', 'WIC')
        newfl = flagsmodified.select(parameter='stationid', values=['WIC'])
        self.assertEqual(len(newfl), 3)

    def test_union(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T23:56:12.654362",
                    endtime="2022-11-22T23:59:12.654362", components=['x', 'y', 'z'], flagtype=0, debug=False)
        # fitting overlap - level 0
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T21:56:12.654362",
                    endtime="2022-11-22T21:59:12.654362", labelid='001', flagtype=3, components=['x', 'y', 'z'],
                    debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T21:58:12.654362",
                    endtime="2022-11-22T22:19:12.654362", labelid='001', flagtype=3, components=['x', 'y', 'z'],
                    debug=False)
        # non-fitting overlap - different label - similar flagtype, similar component
        fl = fl.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:56:12.654362",
                    endtime="2022-11-22T09:59:12.654362", components=['f'], labelid='001', flagtype=1,
                    comment="incredible lightning strike", debug=False)
        fl = fl.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:50:12.654362",
                    endtime="2022-11-22T09:57:12.654362", components=['f'], labelid='002', flagtype=1, comment="spike",
                    debug=False)
        # non-fitting overlap - level 3 different label different flagtype, same component
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T16:36:12.654362",
                    endtime="2022-11-22T16:39:12.654362", components=['x', 'y', 'z'], labelid='021', flagtype=2,
                    debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T16:38:12.654362",
                    endtime="2022-11-22T16:49:12.654362", components=['x', 'y', 'z'], labelid='050', flagtype=1,
                    debug=False)
        # non-fitting overlap - level 1 test similar flagtype, labels - different components (example - marked temperatures or df in additional set)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T17:36:12.654362",
                    endtime="2022-11-22T17:39:12.654362", components=['x', 'y', 'z'], labelid='050', flagtype=3,
                    debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T17:38:12.654362",
                    endtime="2022-11-22T17:49:12.654362", components=['x', 'y', 'z', 'df'], labelid='050', flagtype=3,
                    debug=False)
        # non-fitting overlap - level 2 test similar flagtype, different labels and components (example - unkwon and car label)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T18:36:12.654362",
                    endtime="2022-11-22T18:39:12.654362", components=['x', 'y', 'z'], labelid='050', flagtype=1,
                    debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T18:38:12.654362",
                    endtime="2022-11-22T18:49:12.654362", components=['x', 'y', 'z', 'f'], labelid='090', flagtype=1,
                    debug=False)
        # fitting overlap - directly consecutive
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T19:36:12", endtime="2022-11-22T19:39:12",
                    components=['x', 'y', 'z'], labelid='090', flagtype=1, debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T19:39:12", endtime="2022-11-22T19:49:12",
                    components=['x', 'y', 'z'], labelid='090', flagtype=3, debug=False)
        # fitting overlap - consecutive within sampling rate 1 sec
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T20:36:12", endtime="2022-11-22T20:39:12",
                    components=['x', 'y', 'z'], labelid='090', flagtype=1, debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T20:39:13", endtime="2022-11-22T20:49:12",
                    components=['x', 'y', 'z'], labelid='090', flagtype=1, debug=False)
        # level3 -> 13 data sets with similar sensorid OK                   --> 15 data ->  9 data
        # level2 -> 7---flagtype1, 1---0, 1---2, 4---3  OK                  --> 15 data -> 11 data, with samplingrate 1 -> 10 data
        # level1 -> 5--2---flagtype1, 1---0, 1---2, 2--2---3 OK             --> 15 data -> 13 data, with samplingrate 1 -> 12 data
        # level0 -> 4-1-2--5--2---flagtype1, 1---0, 1---2, 2-1-1--2--2---3  --> 15 data -> 14 data, with samplingrate 1 -> 13 data, no samprate but with typeforce False -> 13 data
        test = fl.union(samplingrate=1, level=0)
        self.assertEqual(len(test), 13)
        test = fl.union(level=0, typeforce=False)
        self.assertEqual(len(test), 13)
        test = fl.union(level=1)
        self.assertEqual(len(test), 13)
        test = fl.union(level=2)
        self.assertEqual(len(test), 11)
        test = fl.union(level=3)
        self.assertEqual(len(test), 9)

    def test_rename_nearby(self):
        fl = Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T23:56:12.654362",
                    endtime="2022-11-22T23:59:12.654362", components=['x', 'y', 'z'], flagtype=0, debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T21:56:12.654362",
                    endtime="2022-11-22T21:59:12.654362", labelid='001', flagtype=3, components=['x', 'y', 'z'],
                    debug=False)
        # non-fitting overlap - different label - similar flagtype, similar component
        fl = fl.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:56:12.654362",
                    endtime="2022-11-22T09:59:12.654362", components=['f'], labelid='001', flagtype=1,
                    comment="incredible lightning strike", debug=False)
        fl = fl.add(sensorid="GSM90_Y1112_0001", starttime="2022-11-22T09:50:12.654362",
                    endtime="2022-11-22T09:57:12.654362", components=['f'], labelid='002', flagtype=1, comment="spike",
                    debug=False)
        # non-fitting overlap - level 3 different label different flagtype, same component
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T16:36:12.654362",
                    endtime="2022-11-22T16:39:12.654362", components=['x', 'y', 'z'], labelid='021', flagtype=2,
                    debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T16:38:12.654362",
                    endtime="2022-11-22T16:49:12.654362", components=['x', 'y', 'z'], labelid='090', flagtype=1,
                    comment='autodetect', debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T17:36:12.654362",
                    endtime="2022-11-22T17:39:12.654362", components=['x', 'y', 'z'], labelid='050', flagtype=1,
                    comment='autodetect', debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T17:38:12.654362",
                    endtime="2022-11-22T17:49:12.654362", components=['x', 'y', 'z', 'df'], labelid='001',
                    label='lightning', comment='incredible lightning', operator='RL', flagtype=3, debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T18:36:12.654362",
                    endtime="2022-11-22T18:39:12.654362", components=['x', 'y', 'z'], labelid='090', flagtype=1,
                    comment='marked elsewhere', debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T18:38:12.654362",
                    endtime="2022-11-22T18:49:12.654362", components=['x', 'y', 'z', 'f'], labelid='090', flagtype=1,
                    comment='autodetect', debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T19:36:12", endtime="2022-11-22T19:39:12",
                    components=['x', 'y', 'z'], labelid='090', flagtype=1, comment='autodetect', debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T19:39:12", endtime="2022-11-22T19:49:12",
                    components=['x', 'y', 'z'], labelid='090', flagtype=1, comment='autodetect', debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T20:36:12", endtime="2022-11-22T20:39:12",
                    components=['x', 'y', 'z'], labelid='090', flagtype=1, comment='autodetect', debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001", starttime="2022-11-22T20:39:13", endtime="2022-11-22T20:49:12",
                    components=['x', 'y', 'z'], labelid='090', flagtype=1, comment='autodetect', debug=False)
        results = fl.rename_nearby(parameter='labelid', values=['001'], searchcomment='auto')
        label = results.flagdict.get('233844116124').get('label')
        self.assertEqual(label, 'unknown disturbance')
        results = fl.rename_nearby(parameter='labelid', values=['001'])
        label = results.flagdict.get('233844116124').get('label')
        self.assertEqual(label, 'lightning strike')


class TestPlot(unittest.TestCase):
    def test_testtimestep(self):
        #from matplotlib.dates import date2num
        v1 = datetime.now(timezone.utc).replace(tzinfo=None)
        v2 = np.datetime64(v1)
        v3 = date2num(v1)
        # can also be used for unittest
        var1 = mp.testtimestep(v1)
        var2 = mp.testtimestep(v2)
        var3 = mp.testtimestep(v3)
        self.assertTrue(var1)
        self.assertTrue(var2)
        self.assertFalse(var3)

    def test_fill_list(self):
        ml = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        v1 = mp.fill_list(ml, 19, 10)
        # can also be used for unittest with np.sum
        # print (np.sum(v1), np.sum(ml)) # +100 for unittest
        self.assertEqual(np.sum(v1), np.sum(ml)+100)


class TestDatabase(unittest.TestCase):
    def test_write(self):
        db = database.DataBank("localhost","maxmustermann","geheim","testdb")
        db.write(teststream, StationID = 'TST')
        self.assertTrue(db.tableexists('Test_0002_0001_0001'), "did not find data table")

    def test_get(self):
        db = database.DataBank("localhost","maxmustermann","geheim","testdb")
        stationid = db.get_string('DATAINFO', 'Test_0002_0001', 'StationID')
        tablename = db.datainfo(teststream.header.get('SensorID'), {'DataComment' : 'Add something'}, None, stationid)
        data = db.get_lines(tablename, 1000)
        sr =  db.get_float('DATAINFO', teststream.header.get('SensorID'), 'DataSamplingRate')
        self.assertEqual(stationid, 'TST')
        self.assertEqual(tablename, 'Test_0002_0001_0001')
        self.assertEqual(len(data), 1000)
        self.assertEqual(sr, 60)

    def test_get_baseline(self):
        db = database.DataBank("localhost","maxmustermann","geheim","testdb")
        test = db.get_baseline('LEMI036_5_0001', date="2021-01-01")
        self.assertEqual(test, {})

    def test_updateselect(self):
        db = database.DataBank("localhost","maxmustermann","geheim","testdb")
        db.update('SENSORS', ['SensorGroup'], ['magnetism'], condition='SensorID="Test_0002_0001"')
        magsenslist = db.select('SensorID', 'SENSORS', 'SensorGroup = "magnetism"')
        self.assertIn('Test_0002_0001', magsenslist)

    def test_read(self):
        db = database.DataBank("localhost","maxmustermann","geheim","testdb")
        data = db.read('Test_0002_0001_0001')
        print (len(data)) # should be 172800
        self.assertEqual(len(data), 1440)

    def test_piers(self):
        db = database.DataBank("localhost","maxmustermann","geheim","testdb")
        pierkeys = ['PierID', 'PierName', 'PierType', 'StationID', 'PierLong', 'PierLat', 'PierAltitude', 'PierCoordinateSystem', 'DeltaDictionary','AzimuthDictionary']
        piervalues1 = ['P1','Karl-Heinzens-Supersockel', 'DI', 'TST', '-34894,584', '310264,811','1088,1366', 'GK M34, EPSG: 31253', 'P2_(2015_(deltaF_-0.13);2017_(deltaF_-0.03);2016_(deltaF_-0.06);2018_(deltaF_-0.09);2019_(deltaF_-0.18);2020_(deltaF_-0.19);2021_(deltaF_-0.17);2022_(deltaF_-0.07);2023_(deltaF_-0.23))','Z64351_180.1559161807, T360-075M1_359.25277, Z64388_272.49390, Z64390_87.46448']
        piervalues2 = ['P2','Hans-Rüdigers-Megasockel', 'DI', 'TST', 461348.00, 5481741.00,101, 'EPSG:25832', 'P2_(2017_(deltaF_-0.85);2016_(deltaI_-0.00019;deltaD_-0.00729;deltaF_-1.33);2019_(deltaF_-0.41);2020_(deltaF_-0.50);2021_(deltaF_-0.77);2022_(deltaF_-0.86);2023_(deltaF_-0.92))','Z64388_268.99712, Z64390_91.41396']
        db.update('PIERS', pierkeys, piervalues1, condition="PierID LIKE 'P1'")
        db.update('PIERS', pierkeys, piervalues2, condition="PierID LIKE 'P2'")
        value = db.get_pier('P1','P2','deltaF')
        self.assertEqual(value, -0.23)
        (long, lat) = db.coordinates('P1')
        self.assertEqual(np.round(long,3), 15.865)

    def test_dbflagging(self):
        db = database.DataBank("localhost","maxmustermann","geheim","testdb")
        self.assertTrue(db.flags_to_delete(parameter="all"))
        fl = flagging.Flags()
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],operator='RL',debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
        fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T19:56:12.654362",endtime="2022-11-22T19:59:12.654362",components=['x','y','z'],groups={'magnetism':['x','y','z','f'],'LEMI':['x','y','z']}, debug=False)
        self.assertTrue(db.flags_to_db(fl))
        fl1 = db.flags_from_db()
        self.assertTrue(db.flags_to_delete(parameter="operator", value="RL"))
        fl2 = db.flags_from_db()
        self.assertEqual(len(fl1), len(fl2)+1)


class TestAbsolutes(unittest.TestCase):
    def test_deg2degminsec(self):
        test = di.deg2degminsec(270.5)
        self.assertEqual(test, '270:30:0')

    def test_absread(self):
        # if this test fails then the following absolute tests will fail as well
        absst = di.abs_read(example6a)
        self.assertEqual(len(absst), 1)

    def test_get_data_list(self):
        absst = di.abs_read(example6a)
        lst = absst[0].get_data_list()
        self.assertEqual(len(lst), 32)
        self.assertIn('# MagPy Absolutes',lst[0])

    def test_get_abs_distruct(self):
        absst = di.abs_read(example6a)
        absdata = absst[0].get_abs_distruct()
        self.assertEqual(len(absdata), 17)

    def test_h_z(self):
        absst = di.abs_read(example6a)
        absdata = absst[0].get_abs_distruct()
        f = 50.00
        h = absdata._h(f,35.)
        z = absdata._z(f,35.)
        f_test = np.round(np.sqrt(h**2 + z**2),2)
        self.assertEqual(f, f_test)

    def test_get_column(self):
        absst = di.abs_read(example6a)
        absdata = absst[0].get_abs_distruct()
        example6amire = 180.1372
        columnmean = np.mean(absdata._get_column('expectedmire'))
        self.assertEqual(example6amire, columnmean)

    def test_get_max_min(self):
        absst = di.abs_read(example6a)
        absdata = absst[0].get_abs_distruct()
        ma = absdata._get_max('vc')
        mi = absdata._get_min('vc')
        self.assertEqual(np.round((ma-mi),2), 231.27)

    def test_corrangle(self):
        absst = di.abs_read(example6a)
        absdata = absst[0].get_abs_distruct()
        a1 = absdata._corrangle(-144.5)
        a2 = absdata._corrangle(575.5)
        a3 = absdata._corrangle(215.5)
        a4 = absdata._corrangle(360.0)
        self.assertEqual(a1, a2)
        self.assertEqual(a2, a3)
        self.assertEqual(a4, 0)

    def test_calcdec_calcinc(self):
        # construct two easy examples woith well known results
        absst = di.abs_read(example6a)
        absdata = absst[0].get_abs_distruct()
        data = data_for_di({'file': example5}, starttime='2018-08-29', endtime='2018-08-30', datatype='both',
                           debug=False)
        valuetest = absdata._check_coverage(data, keys=['x', 'y', 'z', 'f'])
        func = data.header.get('DataFunctionObject')[0]
        absdata = absdata._insert_function_values(func)
        xyzo = False
        hstart = 0.0
        hbasis = 0.0
        ybasis = 0.0
        resultline, decmeanx, decmeany, variocorrold = absdata._calcdec(xstart=20000, ystart=1700, hstart=0.0,
                                                                        hbasis=0.0, ybasis=0.0, deltaD=0.0, usestep=0,
                                                                        scalevalue=None, iterator=0, annualmeans=None,
                                                                        meantime=False, xyzorient=xyzo, residualsign=1,
                                                                        debugmode=False)
        outline, hstart, hbasis = absdata._calcinc(resultline, scalevalue=None, incstart=0.0, deltaI=0.0, iterator=0,
                                                   usestep=0, annualmeans=None, xyzorient=xyzo, decmeanx=decmeanx,
                                                   decmeany=decmeany, variocorrold=variocorrold, residualsign=1,
                                                   debugmode=False)
        self.assertEqual(np.round(outline[1], 4), 64.3705)
        self.assertEqual(np.round(outline[2], 4), 4.3424)

    def test_calcabsolutes(self):
        # construct two easy examples woith well known results
        absst = di.abs_read(example6a)
        absdata = absst[0].get_abs_distruct()
        data = data_for_di({'file':example5}, starttime='2018-08-29',endtime='2018-08-30', datatype='both', debug=False)
        valuetest = absdata._check_coverage(data,keys=['x','y','z','f'])
        func = data.header.get('DataFunctionObject')[0]
        absdata = absdata._insert_function_values(func)
        results = absdata.calcabsolutes(usestep=0, annualmeans=None, printresults=True, debugmode=False,
                              deltaD=0.0, deltaI=0.0, meantime=False, scalevalue=None,
                              variometerorientation='hez', residualsign=1)
        self.assertEqual(np.round(results[1],4), 64.3705)
        self.assertEqual(np.round(results[2],4), 4.3435)  # different to calcdec_caclinc as this is the third iteration step

    def test_diline(self):
        def is_nan(x):
            return (x != x)

        db = database.DataBank("localhost","maxmustermann","geheim","testdb")
        absst = di.abs_read(example6a)
        dbaddsucc = db.diline_to_db(absst, mode="delete", stationid='WIC', debug=True)
        res = db.diline_from_db()
        success = True
        atts = ['time', 'hc', 'vc', 'res', 'opt', 'laser', 'ftime', 'f', 't', 'scaleflux', 'scaleangle', 'azimuth', 'person', 'pier', 'stationid', 'di_inst', 'f_inst', 'fluxgatesensor', 'inputdate']
        # please note: stationid needs to be different as this value has been set by diline_to_db
        atts.remove("stationid")
        for el in atts:
            l1 = absst[0][el]
            l2 = res[0][el]
            if isinstance(l1, (list, tuple)):
                l1 = [el for el in l1 if not np.isnan(el)]
                l2 = [el for el in l2 if not np.isnan(el)]
                if len(l1) == len(l2) and len(l1) == sum([1 for i, j in zip(l1, l2) if i == j]):
                    pass
                else:
                    success = False
            else:
                if is_number(l1) and not np.isnan(l1):
                    if l1 == l2:
                        pass
                    else:
                        success = False
                elif l1 == l2:
                    pass
                elif is_nan(l1) and is_nan(l2):
                    # will work also for strings
                    pass
                else:
                    success = False
        self.assertTrue(success)
        #self.assertTrue(dbaddsucc)

    def test_data_for_di(self):
        # Test1 (basic read):
        # ----------------------
        data = data_for_di(example5, starttime="2018-08-29", datatype='both', debug=True)
        tx1, ty1, tz1, tf1 = data.ndarray[1][0], data.ndarray[2][0], data.ndarray[3][0], data.ndarray[4][0]
        self.assertEqual(np.round(tz1, 2), 43859.29)
        # Test2 (alpha and beta rotations):
        # ----------------------
        data = data_for_di(example5, starttime="2018-08-29", datatype='both', alpha=90, debug=True)
        tx2, ty2, tz2 = data.ndarray[1][0], data.ndarray[2][0], data.ndarray[3][0]
        # Expected X1 = Y2
        # Expected Y1 = -X2
        self.assertEqual(np.round(tx1, 2), np.round(ty2, 2))
        self.assertEqual(np.round(ty1, 2), -np.round(tx2, 2))
        # ----------------------
        data = data_for_di(example5, starttime="2018-08-29", datatype='both', beta=90, debug=True)
        tx3, ty3, tz3 = data.ndarray[1][0], data.ndarray[2][0], data.ndarray[3][0]
        # Expected X1 = -Z3
        # Expected Z1 = X3
        self.assertEqual(np.round(tx1, 2), -np.round(tz3, 2))
        self.assertEqual(np.round(tz1, 2), np.round(tx3, 2))
        # ----------------------
        data = data_for_di(example5, starttime="2018-08-29", datatype='both', alpha=90, beta=90, debug=True)
        tx4, ty4, tz4 = data.ndarray[1][0], data.ndarray[2][0], data.ndarray[3][0]
        # Expected X1 = -Z4
        # Expected Y1 = -X4
        # Expected Z1 = Y4
        self.assertEqual(np.round(tx1, 2), -np.round(tz4, 2))
        self.assertEqual(np.round(ty1, 2), -np.round(tx4, 2))
        self.assertEqual(np.round(tz1, 2), np.round(ty4, 2))
        # Test3 (database usage):
        # ----------------------
        # add the original header to the database and add some artificial compensation and rotation and a DataDeltaDictionary
        db = database.DataBank("localhost", "maxmustermann", "geheim", "testdb")
        data.header['DataID'] = "{}_0001".format(data.header['SensorID'])
        data.header['DataRotationAlpha'] = "2016_90.0,2017_0.0"
        data.header['DataRotationBeta'] = "2018_0.0"
        data.header[
            'DataDeltaValues'] = '{"0": {"st": "1971-11-22 00:00:00", "f": -1.48, "time": "timedelta(seconds=-3.0)", "et": "2018-01-01 00:00:00"}, "1": {"st": "2018-01-01 00:00:00", "f": -1.571, "time": "timedelta(seconds=-3.0)", "et": "2018-09-14 12:00:00"}, "2": {"st": "2018-09-14 12:00:00", "f": -1.571, "time": "timedelta(seconds=1.50)", "et": "2019-01-01 00:00:00"}, "3": {"st": "2019-01-01 00:00:00", "f": -1.631, "time": "timedelta(seconds=-0.30)", "et": "2020-01-01 00:00:00"}, "4": {"st": "2020-01-01 00:00:00", "f": -1.616, "time": "timedelta(seconds=-0.28)", "et": "2021-01-01 00:00:00"}, "5": {"st": "2021-01-01 00:00:00", "f": -1.609, "time": "timedelta(seconds=-0.28)", "et": "2022-01-01 00:00:00"}, "6": {"st": "2022-01-01 00:00:00", "f": -1.655, "time": "timedelta(seconds=-0.33)", "et": "2023-01-01 00:00:00"}, "7": {"st": "2023-01-01 00:00:00", "f": -1.729, "time": "timedelta(seconds=-0.28)"}}'
        # Bias fields in DB are given in !! microT !!. Take of the sign: F_without_bias = F with_bias + DataCompensation
        data.header['DataCompensationX'] = 1
        data.header['DataCompensationY'] = -0.5
        data.header['DataCompensationZ'] = -1
        db.dict_to_fields(data.header, mode='replace')
        # only compensation and delta_f from DataDeltaValues, rotatoion is zero
        data = data_for_di(example5, starttime="2018-08-29", datatype='both', magrotation=True, db=db, debug=True)
        tx5, ty5, tz5, tf5 = data.ndarray[1][0], data.ndarray[2][0], data.ndarray[3][0], data.ndarray[4][0]
        self.assertEqual(np.round(tx1, 2), np.round(tx5 + 1000, 2))
        self.assertEqual(np.round(ty1, 2), np.round(ty5 - 500, 2))
        self.assertEqual(np.round(tz1, 2), np.round(tz5 - 1000, 2))
        self.assertEqual(np.round(tf1, 2), np.round(tf5 + 1.571, 2))
        # ----------------------
        # no compensation but rotation from db and delta_f from DataDeltaValues
        data.header['DataRotationAlpha'] = "2016_90.0,2017_90.0"
        data.header['DataRotationBeta'] = "2018_90.0"
        data.header['DataCompensationX'] = 0.0
        data.header['DataCompensationY'] = 0.0
        data.header['DataCompensationZ'] = 0.0
        db.dict_to_fields(data.header, mode='replace')
        data = data_for_di(example5, starttime="2018-08-29", datatype='both', magrotation=True, db=db, debug=True)
        tx6, ty6, tz6 = data.ndarray[1][0], data.ndarray[2][0], data.ndarray[3][0]
        self.assertEqual(np.round(tx1, 2), -np.round(tz6, 2))
        self.assertEqual(np.round(ty1, 2), -np.round(tx6, 2))
        self.assertEqual(np.round(tz1, 2), np.round(ty6, 2))
        # ----------------------
        # Test5 (optional offset):
        offsets = {'x': 1000.0, 'z': -1000.0}
        data = data_for_di(example5, starttime="2018-08-29", datatype='vario', compensation=True, offset=offsets, db=db,
                           debug=True)
        tx7, ty7, tz7 = data.ndarray[1][0], data.ndarray[2][0], data.ndarray[3][0]
        self.assertEqual(np.round(tx1, 2), np.round(tx7 - 1000, 2))
        self.assertEqual(np.round(tz1, 2), np.round(tz7 + 1000, 2))

    def test_absolute_analysis(self):
        """
        Complete testrun with unittest and some additional runtime tests for absolute_analysis.
        Additionally tests the following methods of the DI package: basically all
        """
        # INDEPTH FORMAT SPECIFIC UNITTESTS are separate
        # - test_format_autodif
        # - test_format_json

        path = os.path.abspath(di.__file__)
        testdidata = os.path.join(os.path.split(path)[0], 'test', 'testdata', 'difiles/')
        varionxy = os.path.join(os.path.split(path)[0], 'test', 'testdata', 'variometer', 'NXY*')
        variosxy = os.path.join(os.path.split(path)[0], 'test', 'testdata', 'variometer', 'SXY*')
        varionxx = os.path.join(os.path.split(path)[0], 'test', 'testdata', 'variometer', 'NXX_vario*')
        scalarnxx = os.path.join(os.path.split(path)[0], 'test', 'testdata', 'scalar', 'NXX_scalar*')
        varioxyzf = os.path.join(os.path.split(path)[0], 'test', 'testdata', 'variometer', 'wic20220810vsec.zip')
        scalarxyzf = os.path.join(os.path.split(path)[0], 'test', 'testdata', 'scalar', 'GP20S32022-08-10.cdf')

        # to be added:
        # -run with all failed analysis and check error reactions (add try/except)

        def vdi(absresult):
            """
            Extract some parameters of the results for verification with known results
            """
            if len(absresult) > 0:
                incl = np.round(absresult.ndarray[1][0], 4)
                decl = np.round(absresult.ndarray[2][0], 4)
                fstr = np.round(absresult.ndarray[3][0], 2)
                scalev = np.round(absresult.ndarray[6][0], 3)
                s0d = np.round(absresult.ndarray[7][0], 2)
                ze = np.round(absresult.ndarray[11][0], 2)
                str2 = absresult.ndarray[17][0]
                #print("Theodolite", str2)
                return {"inc": incl, "dec": decl, "f": fstr, "sv": scalev, "s0d": s0d, "ze": ze}
            else:
                return {}

        def check_base(data, basevalues, comps='HDZ'):
            # get basevalues
            ti = basevalues._get_column('time')
            bh = basevalues._get_column('dx')
            be = basevalues._get_column('dy')
            bz = basevalues._get_column('dz')
            # perform a baseline correction with this basevalues -> will lead to a hdz data files
            data = data.simplebasevalue2stream([bh[0], be[0], bz[0]], basecomp=comps)
            # select IDF from the baseline corrected dataset and compare with absolutes
            if comps == 'HDZ':
                data = data.hdz2xyz()
            #data = data.xyz2idf()
            basevalues = basevalues.idf2xyz()
            idx = data.findtime(ti[0])
            #dinc = data.ndarray[1][idx]
            #ddec = data.ndarray[2][idx]
            #df = data.ndarray[3][idx]
            bax = basevalues._get_column('x')
            bay = basevalues._get_column('y')
            baz = basevalues._get_column('z')
            dax = data.ndarray[1][idx]
            day = data.ndarray[2][idx]
            daz = data.ndarray[3][idx]
            # should be equal within 0.01 nT (which is the IAGA vraiofile resolution)
            delta = 0.01
            message = "not almost equal."
            # assert function() to check if values are almost equal
            self.assertAlmostEqual(np.round(bax[0],2),np.round(dax,2), None, message, delta)
            self.assertAlmostEqual(np.round(bay[0],2),np.round(day,2), None, message, delta)
            self.assertAlmostEqual(np.round(baz[0],2),np.round(daz,2), None, message, delta)
            #print(np.round(ainc[0], 6), np.round(adec[0], 6), np.round(af[0], 6), np.round(dinc, 6), np.round(ddec, 6),
            #      np.round(df, 6))

        ## FIRST TEST: READ DATA AND USE OPTIONS NOT TESTED BELOW
        # ----------------------------------------------------------------------

        # Corrections: alpha, beta, deltaF, deltaD, deltaI, compensation, magrotation;
        # Residual method: residualsign
        # Thresholds: expD, expI, expT
        # Archiving successful analysis: movetoarchive and dbadd; TODO code needs to be written and tested (start and endtime seem not to be
        # considered for filelist),

        # movetoarchive
        # basevalues = absolute_analysis('DIDATA', {'file':example5, 'db':(db,'WIC_1_0001_0001')}, example5, db=db, starttime="2018-08-28", endtime="2018-08-30", movetoarchive="/home/leon/Tmp/")
        # basevalues = absolute_analysis("/home/leon/Tmp/2018-08-29_07-42-00_A2_WIC.txt", {'file':example5, 'db':(db,'WIC_1_0001_0001')}, example5, db=db, starttime="2018-08-28", endtime="2018-08-30", movetoarchive="/tmp")

        ## SECOND TEST: GET VALUES FROM ANALYSIS AND COMPARE WITH EXPECTED VALUES
        # ----------------------------------------------------------------------
        # EXPECTED = {"NXY" : {"S0D" : 16.815, "HD" : -19.762, "HE" : -93.382, "S0Z" : 15.972, "ZE" : -93.267, "SV" : 1.000, "D" : 311.27127, "I" : 85.80933, "F": 56326.961},
        #            "SXX" : {"S0D" : 6.8, "HD" : 0.7, "HE" : 3.8, "S0Z" : 7.2, "ZE" : 4.3, "SV" : 0.981, "D" : '338:13:41.4', "I" : -65},
        #            "SXY" : {"S0D" : 5.123, "HD" : 3.826, "HE" : 0.778, "S0Z" : 6.046, "ZE" : 1.916, "SV" : 0.981, "D" : 15.68170, "I" : -57.19036, "F": 37820.98}}

        secondsuccess = True
        expnxy = {'inc': 85.8093, 'dec': 311.2713, 'f': 56326.93, 'sv': 1.0, 's0d': 16.82, 'ze': -93.27}
        expsxy = {'inc': -57.1904, 'dec': 15.6817, 'f': 37821.0, 'sv': 999.0, 's0d': 5.12, 'ze': 1.92}
        expnxx = {'inc': 64.3155, 'dec': 4.1843, 'f': 48585.45, 'sv': 0.995, 's0d': -4.42, 'ze': 16.27}
        expxyzf = {'inc': 64.4645, 'dec': 4.8976, 'f': 48836.75, 'sv': 0.991, 's0d': -3.26, 'ze': -42.01}

        nabsresult = di.absolute_analysis(testdidata, varionxy, varionxy, diid='NXY.txt', stationid='NXY', pier='2',
                                       alpha=0.0, deltaF=0.0)
        sabsresult = di.absolute_analysis(testdidata, variosxy, variosxy, diid='SXY.txt', stationid='SXY', pier='A1',
                                       alpha=0.0, deltaF=0.0)
        nxxabsresult = di.absolute_analysis(testdidata, varionxx, scalarnxx, diid='NXX.txt', stationid='NXX', pier='A2',
                                         alpha=0.0, deltaF=-0.5)
        #print(vdi(nxxabsresult))
        xyzfabsresult = di.absolute_analysis(testdidata, varioxyzf, scalarxyzf, diid='WIC.txt', stationid='WIC', pier='A2',
                                          variometerorientation='XYZ', expD=5.0, expI=64.0, expT=2.0)
        if not expnxy == vdi(nabsresult):
            secondsuccess = False
            print(" absolute_analysis second unittest: NXY test failed")
        if not expsxy == vdi(sabsresult):
            secondsuccess = False
            print(" absolute_analysis second unittest: SXY test failed")
        if not expnxx == vdi(nxxabsresult):
            secondsuccess = False
            print(" absolute_analysis second unittest: NXX test failed")
        if not expxyzf == vdi(xyzfabsresult):
            secondsuccess = False
            print(" absolute_analysis second unittest: XYZF test failed")
        self.assertTrue(secondsuccess)

        ## THIRD TEST: APPLY DETERMINED BASEVALUES TO VARIOMETER DATA - CHECK IF DEC, INC and F are identically found in VARIO and SCALAR DATA
        # ----------------------------------------------------------------------
        # Test with HEZ
        basevalues1 = di.absolute_analysis(example6a, example5, example5, stationid='WIC')
        data1 = read(example5)
        check_base(data1, basevalues1)
        # Test with XYZF
        basevalues2 = di.absolute_analysis(testdidata, varioxyzf, scalarxyzf, diid='A2_WIC.txt', stationid='WIC',
                                        pier='A2', variometerorientation='XYZ')
        data2 = read(varioxyzf)
        check_base(data2, basevalues2, comps='XYZ')


class TestActivity(unittest.TestCase):

    def test_k_fmi(self):
        # activity module
        teststream = create_activityverificationstream()
        minstream = teststream.filter()
        k = activity.K_fmi(minstream, debug=False)
        meank = k.mean('var1')
        self.assertEqual(meank, 6.625)

    def test_storm_determination(self):
        # activity module
        teststream = create_activityverificationstream()
        ssc = teststream.ndarray[0][121800]
        teststream = teststream.filter(missingdata='interpolate', noresample=True)
        for method in ['AIC','DWT2','MODWT','FDM']:
            print ("Testing technique: ", method)
            k = activity.seek_storm(teststream.trim(starttime='2022-11-22',endtime='2022-11-23'), satdata_1m=None, satdata_5m=None, verbose=False, method=method)
            stormfound = k[0]
            #self.assertTrue(stormfound)
            if stormfound:
                stormdict = k[1]
                st = stormdict.get(0).get('starttime')
                et = stormdict.get(0).get('endtime')
                # check if ssc is within found timerange
                self.assertGreaterEqual(ssc, st)
                self.assertLessEqual(ssc, et)
            else:
                print (" No storm found - check")



if __name__ == "__main__":
    unittest.main(verbosity=2)