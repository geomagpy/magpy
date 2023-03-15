import unittest
import math
import magpy.lib.format_autodif as auto_format
import magpy.lib.format_abs_magpy as abs_format

'''absst = absRead(path_or_url=abspath, azimuth=azimuth, output='DIListStruct')
stream = elem.getAbsDIStruct()
variostr = read(variopath)
variostr = variostr.rotation(alpha=varioorientation_alpha, beta=varioorientation_beta)
vafunc = variostr.interpol(['x', 'y', 'z'])
stream = stream._insert_function_values(vafunc)
scalarstr = read(scalarpath)
scfunc = scalarstr.interpol(['f'])
stream = stream._insert_function_values(scfunc, funckeys=['f'], offset=deltaF)
result = stream.calcabsolutes(usestep=2, annualmeans=[20000, 1200, 43000], printresults=True)
'''

class TestFormatReaderMK2(unittest.TestCase):

    abs_file_1 = 'testdata/autodif/mk2/compare/auto/20200801_00-11-13.abs'
    ref_file_1 = './testdata/autodif/mk2/compare/magpy/2020-08-01_00-11-13_DO4_DOU.txt'
    abs_file_2 = 'testdata/autodif/mk2/compare/auto/20200801_00-41-03.abs'
    ref_file_2 = './testdata/autodif/mk2/compare/magpy/2020-08-01_00-41-03_DO4_DOU.txt'

    def  test_errors_raised(self):
        self.assertRaises(NotImplementedError, lambda: auto_format.readAUTODIFABS(self.abs_file_1))
        self.assertRaises(NotImplementedError, lambda: auto_format.readAUTODIFABS(self.abs_file_1,output='AbsoluteDIStruct'))

    def test_compare_structure_1(self):
        abs_list = auto_format.readAUTODIFABS(self.abs_file_1, output='DIListStruct')
        ref_list =  abs_format.readMAGPYNEWABS(self.ref_file_1,output='DIListStruct')
        # NAN can not be used by equal so replaced by 999999
        ref_list[0].vc = [99999 if math.isnan(i) else i for i in ref_list[0].vc]
        abs_list[0].vc = [99999 if math.isnan(i) else i for i in abs_list[0].vc]

        #declination tests UE DW DE UW
        self.assertSequenceEqual(ref_list[0].time[4:12], abs_list[0].time[4:12], 'timestamps decl mes not equal')
        self.assertSequenceEqual(ref_list[0].hc[4:12], abs_list[0].hc[4:12], 'hc decl mes not equal')
        self.assertSequenceEqual(ref_list[0].vc[4:12], abs_list[0].vc[4:12], 'vc decl mes not equal')
        #inclination tests US DN DS UN
        self.assertSequenceEqual(ref_list[0].time[16:24], abs_list[0].time[16:24], 'time incl mes not equal')
        self.assertSequenceEqual(ref_list[0].vc[16:24], abs_list[0].vc[16:24], 'vc incl mes not equal')
        self.assertSequenceEqual(ref_list[0].hc[16:24], abs_list[0].hc[16:24], 'hc incl mes not equal')
        self.assertSequenceEqual(ref_list[0].vc[:24], abs_list[0].vc, 'vertical circle values are not equal')
        self.assertEqual(ref_list[0].azimuth, abs_list[0].azimuth, 'azimuth are not equal')
        self.assertEqual(ref_list[0].pier, abs_list[0].pier, 'azimuth are not equal')
        #target measurements time is not tested because it is not equal also vc because it is not registered
        to_compare_1 = ref_list[0].hc[0:4].copy()
        to_compare_2 = ref_list[0].hc[12:16].copy()
        to_compare_1.reverse()
        to_compare_2.reverse()
        self.assertSequenceEqual(to_compare_1, abs_list[0].hc[0:4], 'first hc set target not equal')
        self.assertSequenceEqual(to_compare_2, abs_list[0].hc[12:16], 'second hc set target not equal')


    def test_compare_structure_2(self):
        abs_list = auto_format.readAUTODIFABS(self.abs_file_2, output='DIListStruct')
        ref_list =  abs_format.readMAGPYNEWABS(self.ref_file_2,output='DIListStruct')
        # NAN can not be used by equal so replaced by 999999
        ref_list[0].vc = [99999 if math.isnan(i) else i for i in ref_list[0].vc]
        abs_list[0].vc = [99999 if math.isnan(i) else i for i in abs_list[0].vc]

        #declination tests UE DW DE UW
        self.assertSequenceEqual(ref_list[0].time[4:12], abs_list[0].time[4:12], 'timestamps decl mes not equal')
        self.assertSequenceEqual(ref_list[0].hc[4:12], abs_list[0].hc[4:12], 'hc decl mes not equal')
        self.assertSequenceEqual(ref_list[0].vc[4:12], abs_list[0].vc[4:12], 'vc decl mes not equal')
        #inclination tests US DN DS UN
        self.assertSequenceEqual(ref_list[0].time[16:24], abs_list[0].time[16:24], 'time incl mes not equal')
        self.assertSequenceEqual(ref_list[0].vc[16:24], abs_list[0].vc[16:24], 'vc incl mes not equal')
        self.assertSequenceEqual(ref_list[0].hc[16:24], abs_list[0].hc[16:24], 'hc incl mes not equal')
        self.assertSequenceEqual(ref_list[0].vc[:24], abs_list[0].vc, 'vertical circle values are not equal')
        self.assertEqual(ref_list[0].azimuth, abs_list[0].azimuth, 'azimuth are not equal')
        self.assertEqual(ref_list[0].pier, abs_list[0].pier, 'azimuth are not equal')
        #target measurements time is not tested because it is not equal also vc because it is not registered
        to_compare_1 = ref_list[0].hc[0:4].copy()
        to_compare_2 = ref_list[0].hc[12:16].copy()
        to_compare_1.reverse()
        to_compare_2.reverse()
        self.assertSequenceEqual(to_compare_1, abs_list[0].hc[0:4], 'first hc set target not equal')
        self.assertSequenceEqual(to_compare_2, abs_list[0].hc[12:16], 'second hc set target not equal')



