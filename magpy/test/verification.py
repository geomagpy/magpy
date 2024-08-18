import unittest
import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2
from magpy.stream import *
from magpy.core.flagging import *
from magpy.core import database


def create_verificationstream(startdate=datetime(2022, 11, 22)):
    teststream = DataStream()
    array = [[] for el in DataStream().KEYLIST]
    array[1] = [20000]*720
    array[1].extend([22000]*720)
    array[1] = np.asarray(array[1])
    array[2] = np.asarray([0] * 1440)
    array[3] = np.asarray([20000] * 1440)
    # array[4] = np.sqrt((x*x) + (y*y) + (z*z))
    array[0] = np.asarray([startdate + timedelta(minutes=i) for i in range(0, len(array[1]))])
    array[KEYLIST.index('sectime')] = np.asarray(
        [startdate + timedelta(minutes=i) for i in range(0, len(array[1]))]) + timedelta(minutes=15)
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
        self.assertEqual(2, 1)

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
        val1 = [np.round(teststream.ndarray[1][0], 8), np.round(teststream.ndarray[2][0],8), np.round(teststream.ndarray[3][0],8)]
        val2 = [np.round(xyz.ndarray[1][0],8), np.round(xyz.ndarray[2][0],8), np.round(xyz.ndarray[3][0],8)]
        self.assertEqual(meand1, meand2)
        self.assertEqual(teststream.length(), xyz.length())
        self.assertEqual(val1, val2)

    def test_copy_column(self):
        orgstream = teststream.copy()
        self.assertEqual(orgstream.length(), teststream.length())

    def test_det_trange(self):
        self.assertEqual(2, 1)

    def test_drop_column(self):
        cpstream = teststream.copy()
        dropstream = cpstream._drop_column('z')
        self.assertEqual(len(dropstream.ndarray[3]), 0)

    def test_get_column(self):
        col = teststream._get_column('z')
        self.assertEqual(np.mean(col), 20000)

    def test_get_key_headers(self):
        keys = teststream._get_key_headers()
        self.assertEqual(keys, ['x', 'y', 'z', 'sectime'])

    def test_get_key_names(self):
        keysdic = teststream._get_key_names()
        self.assertEqual(keysdic.get('X'), 'x')

    def test_get_max(self):
        max = teststream._get_max('x')
        self.assertEqual(max, 22000)

    def test_get_min(self):
        max = teststream._get_min('x')
        self.assertEqual(max, 20000)

    def test_get_variance(self):
        var = teststream._get_variance('x')
        self.assertEqual(var, 1000000)

    def test_move_column(self):
        cpstream = teststream.copy()
        mvstream = cpstream._move_column('x', 'z')
        self.assertEqual(21000, np.mean(mvstream.ndarray[3]))

    # def test_print_key_headers(self):
    #    self.assertEqual(2, 1)

    def test_put_column(self):
        cpstream = teststream.copy()
        col = cpstream._get_column('x')
        pustream = cpstream._put_column(col, 'var1')
        self.assertEqual(len(pustream._get_column('var1')), 1440)

    def test_remove_nancolumns(self):
        self.assertEqual(2, 1)

    def test_select_keys(self):
        xystream = teststream._select_keys(keys=['x','y'])
        self.assertEqual(len(xystream.ndarray[1]), 1440)
        self.assertEqual(len(xystream.ndarray[2]), 1440)
        self.assertEqual(len(xystream.ndarray[3]), 0)

    def test_select_timerange(self):
        self.assertEqual(2, 1)

    def test_tau(self):
        self.assertEqual(2, 1)

    def test_add(self):
        self.assertEqual(2, 1)

    def test_aic_calc(self):
        self.assertEqual(2, 1)

    def test_amplitude(self):
        amp = teststream.amplitude('x')
        self.assertEqual(amp, 2000)

    def test_baseline(self):
        self.assertEqual(2, 1)

    def test_bc(self):
        self.assertEqual(2, 1)

    def test_calc_f(self):
        fstream = teststream.calc_f()
        self.assertEqual(len(fstream.ndarray[4]), 1440)
        fval = fstream.ndarray[4][0]
        self.assertEqual(fval, np.sqrt(20000*20000 + 20000*20000))

    def test_compensation(self):
        self.assertEqual(2, 1)

    def test_cut(self):
        self.assertEqual(2, 1)

    def test_dailymeans(self):
        dmt = teststream.calc_f()
        dm = dmt.dailymeans()
        self.assertEqual(len(dm), 1)

    def test_delta_f(self):
        self.assertEqual(2, 1)

    def test_determine_rotationangles(self):
        cpstream = teststream.copy()  # rotation is destructive
        idstream = teststream.copy()  # conversion is destructive
        idfstream = idstream.xyz2idf()
        meani = idfstream.mean('x')
        meand = idfstream.mean('y')
        rotstream = cpstream.rotation(alpha=45, beta=45)  # v in z upwards
        alpha, beta = rotstream.determine_rotationangles(referenceD=meand,referenceI=meani)
        self.assertEqual(np.round(alpha,1), -45)
        self.assertEqual(np.round(beta,1), -45)

    def test_dict2stream(self):
        self.assertEqual(2, 1)

    def test_differentiate(self):
        self.assertEqual(2, 1)

    def test_dropempty(self):
        self.assertEqual(2, 1)

    def test_dwt_calc(self):
        self.assertEqual(2, 1)

    def test_end(self):
        end = teststream.end()
        self.assertEqual(end, datetime(2022,11,22,23,59))

    def test_extend(self):
        self.assertEqual(2, 1)

    def test_extract(self):
        extstream = teststream.extract("x" , 20000, ">")
        self.assertEqual(len(extstream), 720)

    def test_extract_headerlist(self):
        self.assertEqual(2, 1)

    def test_extrapolate(self):
        t1 = teststream.trim(starttime='2022-11-22T09:00:00', endtime='2022-11-22T14:00:00')
        ex1 = t1.extrapolate(starttime='2022-11-22T07:00:00', endtime='2022-11-22T16:00:00',method='spline')
        self.assertEqual(len(ex1), 538)

    def test_filter(self):
        self.assertEqual(2, 1)

    def test_fillempty(self):
        self.assertEqual(2, 1)

    def test_findtime(self):
        self.assertEqual(2, 1)

    def test_fit(self):
        self.assertEqual(2, 1)

    def test_func2header(self):
        self.assertEqual(2, 1)

    def test_func2stream(self):
        self.assertEqual(2, 1)

    def test_get_fmi_array(self):
        self.assertEqual(2, 1)

    def test_get_gaps(self):
        self.assertEqual(2, 1)

    def test_get_key_name(self):
        self.assertEqual(2, 1)

    def test_get_key_unit(self):
        self.assertEqual(2, 1)

    def test_get_sampling_period(self):
        self.assertEqual(2, 1)

    def test_harmfit(self):
        self.assertEqual(2, 1)

    def test_hdz2xyz(self):
        # tested by _conversion
        self.assertEqual(1, 1)

    def test_idf2xyz(self):
        # tested by _conversion
        self.assertEqual(1, 1)

    def test_integrate(self):
        self.assertEqual(2, 1)

    def test_interpol(self):
        self.assertEqual(2, 1)

    def test_interpolate_nans(self):
        self.assertEqual(2, 1)

    def test_length(self):
        # Tests also _get_sampling_period
        self.assertEqual(teststream.length()[0], 1440)
        self.assertEqual(len(teststream), 1440)

    def test_mean(self):
        pmeanx = teststream.mean('x')
        self.assertEqual(pmeanx, 21000)

    def test_modwt_calc(self):
        self.assertEqual(2, 1)

    def test_multiply(self):
        mstream = teststream.multiply({'x':2})
        pmeanx = mstream.mean('x')
        self.assertEqual(pmeanx, 42000)

    def test_offset(self):
        self.assertEqual(2, 1)

    def test_randomdrop(self):
        self.assertEqual(2, 1)

    def test_remove(self):
        self.assertEqual(2, 1)

    def test_resample(self):
        self.assertEqual(2, 1)

    def test_rotation(self):
        pmeanx = teststream.mean('x')
        pmeanz = teststream.mean('z')
        cpstream = teststream.copy()
        rotstream = cpstream.rotation(alpha=45, beta=135)
        meanz = rotstream.mean('z')
        self.assertEqual(np.sqrt(pmeanx**2 + pmeanz**2), np.abs(meanz))

    def test_samplingrate(self):
        # Tests also _get_sampling_period
        self.assertEqual(teststream.samplingrate(), 60)

    def test_simplebasevalue2stream(self):
        self.assertEqual(2, 1)

    def test_smooth(self):
        self.assertEqual(2, 1)

    def test_sorting(self):
        self.assertEqual(2, 1)

    def test_start(self):
        start = teststream.start()
        self.assertEqual(start, datetime(2022,11,22))

    def test_steadyrise(self):
        self.assertEqual(2, 1)

    def test_stream2dict(self):
        self.assertEqual(2, 1)

    def test_trim(self):
        t1 = teststream.trim(starttime='2022-11-22T09:00:00', endtime='2022-11-22T14:00:00')
        self.assertEqual(len(t1), 300)

    def test_use_sectime(self):
        self.assertEqual(2, 1)

    def test_write(self):
        self.assertEqual(2, 1)

    def test_xyz2hdz(self):
        # tested by _conversion
        self.assertEqual(1, 1)

    def test_xyz2idf(self):
        # tested by _conversion
        self.assertEqual(1, 1)


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

if __name__ == "__main__":
    unittest.main(verbosity=2)