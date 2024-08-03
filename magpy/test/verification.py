import unittest
import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2
from magpy.stream import *


def create_verificationstream(startdate=datetime(2022, 11, 22)):
    teststream = DataStream()
    array = [[] for el in DataStream().KEYLIST]
    array[1] = np.asarray([20000] * 1440)
    array[2] = np.asarray([0] * 1440)
    array[3] = np.asarray([20000] * 1440)
    # array[4] = np.sqrt((x*x) + (y*y) + (z*z))
    array[0] = np.asarray([startdate + timedelta(minutes=i) for i in range(0, len(array[1]))])
    array[KEYLIST.index('sectime')] = np.asarray(
        [startdate + timedelta(minutes=i) for i in range(0, len(array[1]))]) + timedelta(minutes=15)
    teststream = DataStream(header={'SensorID': 'Test_0002_0001'}, ndarray=np.asarray(array, dtype=object))
    minstream = teststream.filter()
    teststream.header['col-x'] = 'X'
    teststream.header['col-y'] = 'Y'
    teststream.header['col-z'] = 'Z'
    teststream.header['col-f'] = 'F'
    teststream.header['unit-col-x'] = 'nT'
    teststream.header['unit-col-y'] = 'nT'
    teststream.header['unit-col-z'] = 'nT'
    teststream.header['unit-col-f'] = 'nT'
    return teststream


teststream = create_verificationstream()


class TestStream(unittest.TestCase):

    def test_samplingrate(self):
        # Tests also _get_sampling_period
        self.assertEqual(teststream.samplingrate(), 60)

    def test_rotation(self):
        pmeanx = teststream.mean('x')
        pmeany = teststream.mean('y')
        rotstream = teststream.rotation(alpha=45)
        meanx = rotstream.mean('x')
        meany = rotstream.mean('y')
        meanz = rotstream.mean('z')
        print (meanx,meany,meanz,pmeanx,pmeany)
        self.assertEqual(meanx, pmeany)
        self.assertEqual(meany, pmeanx)

    """
    def test_rotation(self):
        self.assertEqual(is_even(2), True)

    def test_calc_f(self):
        fstream = teststream.calc_f()
        self.assertEqual(is_even(3), False)

    def test_delta_f(self):
        self.assertEqual(is_even(-2), True)

    def test_negative_odd_number(self):
        self.assertEqual(is_even(-3), False)
    """


if __name__ == "__main__":
    unittest.main(verbosity=2)