import unittest
import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2
from magpy.stream import *


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
    return teststream


teststream = create_verificationstream()
print (teststream.ndarray)


class TestStream(unittest.TestCase):

    def test_aic(self):
        self.assertEqual(2, 1)

    def test_convertstream(self):
        self.assertEqual(2, 1)

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
        print("Variance", var)
        self.assertEqual(var, 1000000)

    def test_move_column(self):
        cpstream = teststream.copy()
        mvstream = cpstream._move_column('x', 'z')
        self.assertEqual(21000, np.mean(mvstream.ndarray[3]))

    # def test_print_key_headers(self):
    #    self.assertEqual(2, 1)

    def test_put_column(self):
        self.assertEqual(2, 1)

    def test_remove_nancolumns(self):
        self.assertEqual(2, 1)

    def test_select_keys(self):
        kstream = teststream._select_keys(keys=['x','y'])
        print (kstream)
        self.assertEqual(2, 1)

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
        self.assertEqual(2, 1)

    def test_compensation(self):
        self.assertEqual(2, 1)

    def test_cut(self):
        self.assertEqual(2, 1)

    def test_dailymeans(self):
        self.assertEqual(2, 1)

    def test_delta_f(self):
        self.assertEqual(2, 1)

    def test_determine_rotationangles(self):
        self.assertEqual(2, 1)

    def test_dict2stream(self):
        self.assertEqual(2, 1)

    def test_differentiate(self):
        self.assertEqual(2, 1)

    def test_dropempty(self):
        self.assertEqual(2, 1)

    def test_dwt_calc(self):
        self.assertEqual(2, 1)

    def test_end(self):
        self.assertEqual(2, 1)

    def test_extend(self):
        self.assertEqual(2, 1)

    def test_extract(self):
        self.assertEqual(2, 1)

    def test_extract_headerlist(self):
        self.assertEqual(2, 1)

    def test_extrapolate(self):
        self.assertEqual(2, 1)

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
        self.assertEqual(2, 1)

    def test_idf2xyz(self):
        self.assertEqual(2, 1)

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
        self.assertEqual(2, 1)

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
        pmeany = teststream.mean('y')
        rotstream = teststream.rotation(alpha=45)
        meanx = rotstream.mean('x')
        meany = rotstream.mean('y')
        meanz = rotstream.mean('z')
        print(meanx, meany, meanz, pmeanx, pmeany)
        self.assertEqual(meanx, pmeany)
        self.assertEqual(meany, pmeanx)

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
        self.assertEqual(2, 1)

    def test_steadyrise(self):
        self.assertEqual(2, 1)

    def test_stream2dict(self):
        self.assertEqual(2, 1)

    def test_trim(self):
        self.assertEqual(2, 1)

    def test_use_sectime(self):
        self.assertEqual(2, 1)

    def test_write(self):
        self.assertEqual(2, 1)

    def test_xyz2hdz(self):
        self.assertEqual(2, 1)

    def test_xyz2idf(self):
        self.assertEqual(2, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)