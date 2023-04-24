#!/usr/bin/env python
"""
MagPy - Basic flagging tests including durations  
"""
from magpy.stream import *
import magpy.core.flags as flg
#import flags as flg
import datetime

"""
APPLICTAIONS:
Test Program for all flagging related tools and methods 

Test all methods:
- union (combine consectutive and overlapping flagging info)

"""


def test_union():
   # issue: flags are stored in database, loading flags directly afterwards is fine, a day later flags have vanished from data database
   """
   Approch:
   - Load data set with overlapping flags
   - Combine overlaps and keep newest inputs
   """
   flaglist = [
       [datetime.datetime(2022, 5, 3, 2, 28, 11, 710285), datetime.datetime(2022, 5, 4, 1, 9, 39, 790306), 'f', 0,
        'ok BL', 'GP20S3NSS2_012201_0001', datetime.datetime(2023, 3, 3, 14, 50, 46, 281999)],
       [datetime.datetime(2022, 5, 3, 2, 33, 29, 734155), datetime.datetime(2022, 5, 4, 0, 4, 18, 822204), 'f', 3,
        'data missing BL', 'GP20S3NSS2_012201_0001', datetime.datetime(2022, 5, 31, 12, 55, 39, 13425)],
       [datetime.datetime(2022, 5, 3, 3, 41, 23, 681463), datetime.datetime(2022, 5, 3, 3, 42, 59, 676068), 'f', 3,
        'vehicle RL', 'GP20S3NSS2_012201_0001', datetime.datetime(2023, 4, 18, 9, 56, 55, 227970)],
       [datetime.datetime(2022, 5, 3, 3, 41, 38, 680200), datetime.datetime(2022, 5, 3, 3, 41, 43, 659140), 'f', 1,
        'aof - threshold 5 window 600.0 sec', 'GP20S3NSS2_012201_0001',
        datetime.datetime(2022, 5, 3, 3, 48, 36, 479796)],
       [datetime.datetime(2022, 5, 3, 3, 42, 40, 654020), datetime.datetime(2022, 5, 3, 3, 42, 50, 682860), 'f', 1,
        'aof - threshold 5 window 600.0 sec', 'GP20S3NSS2_012201_0001',
        datetime.datetime(2022, 5, 3, 3, 48, 36, 479796)]]
   fl = flg.flags(flaglist)
   combflaglist = fl.union(debug=True)
   fl.stats()
   combflaglist.stats()
   if len(combflaglist) == 3:
       return True
   print("Union test problem - expected result not obtained")
   return False
   

   
# test 1
try:
    succ = test_union(fl)
    print("Union test successfully finished")
except:
    print("Union test failed")
    sys.exit(1)

sys.exit()

