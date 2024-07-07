# -*- coding: utf-8 -*-

"""
methods contain all methods which do not fit to a magpy specific class

the following methods are contained:
- is_number(variable)    :    returns True if variable is float or int
- testtime(variable)     :    returns datetime object if variable can be converted to it
- mask_nan(array)        :    returns an array without nan or empty elements

"""
#from magpy.stream import * # os, num2date
import numpy as np
from datetime import datetime
from matplotlib.dates import num2date, date2num

def is_number(s):
    """
    DESCRIPTION
    Test whether s is a number
    """
    if str(s) in ['', 'None', None]:
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False
    except:
        return False

def testtime(time):
        """
        Check the date/time input and returns a datetime object if valid:

        IMPORTANT: testtime will convert datetime64 to datetime objects. One might change that in the future

        ! Use UTC times !

        - accepted are the following inputs:
        1) absolute time: as provided by date2num
        2) strings: 2011-11-22 or 2011-11-22T11:11:00
        3) datetime objects by datetime.datetime e.g. (datetime(2011,11,22,11,11,00)

        """

        if isinstance(time, float) or isinstance(time, int):
            try:
                timeobj = num2date(time).replace(tzinfo=None)
            except:
                raise TypeError
        elif isinstance(time, str): # test for str only in Python 3 should be basestring for 2.x
            try:
                timeobj = datetime.strptime(time,"%Y-%m-%d")
            except:
                try:
                    timeobj = datetime.strptime(time,"%Y-%m-%dT%H:%M:%S")
                except:
                    try:
                        timeobj = datetime.strptime(time,"%Y-%m-%d %H:%M:%S.%f")
                    except:
                        try:
                            timeobj = datetime.strptime(time,"%Y-%m-%dT%H:%M:%S.%f")
                        except:
                            try:
                                timeobj = datetime.strptime(time,"%Y-%m-%d %H:%M:%S")
                            except:
                                try:
                                    # Not happy with that but necessary to deal
                                    # with old 1000000 micro second bug
                                    timearray = time.split('.')
                                    if timearray[1] == '1000000':
                                        timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")+timedelta(seconds=1)
                                    else:
                                        # This would be wrong but leads always to a TypeError
                                        timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")
                                except:
                                    try:
                                        timeobj = num2date(float(time)).replace(tzinfo=None)
                                    except:
                                        raise TypeError
        elif isinstance(time, np.datetime64):
            unix_epoch = np.datetime64(0, 's')
            one_second = np.timedelta64(1, 's')
            seconds_since_epoch = (time - unix_epoch) / one_second
            timeobj = datetime.utcfromtimestamp(seconds_since_epoch)
        elif not isinstance(time, datetime):
            raise TypeError
        else:
            timeobj = time

        return timeobj

def maskNAN(column):
    """
    Tests for NAN values in column and usually masks them
    """

    numeric = False
    datetype = False
    if len(column) > 0:
        if is_number(column[0]):
            numeric = True
        elif isinstance(column[0], (datetime,np.datetime64)):
            datetype = True
    else:
        return column

    if numeric:
        try: # Test for the presence of nan values to it
            column = np.asarray(column).astype(float)
            val = np.mean(column)
            if np.isnan(val): # found at least one nan value
                for el in column:
                    if not np.isnan(el): # at least on number is present - use masked_array
                        num_found = True
                if num_found:
                    mcolumn = np.ma.masked_invalid(column)
                    column = mcolumn
                else:
                    numdat = False
                    return []
        except:
            return []
    elif datetype:
        try:
            indicies = np.argwhere(column=="")
            mask = np.zeros(len(column),dtype=bool)
            if indicies.size > 0:
                mask[indicies[0]] = True
                column = column[~mask]
        except:
            pass
    else:
        try:
            column = np.ma.masked_where(column=="",column)
        except:
            pass

    return column


if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: Methods PACKAGE")
    print("THIS IS A TEST RUN OF THE MAGPY.CORE METHODS PACKAGE.")
    print("All main methods will be tested. This may take a while.")
    print("If errors are encountered they will be listed at the end.")
    print("Otherwise True will be returned")
    print("----------------------------------------------------------")
    print()

    errors = {}
    testdate = "1971-11-22T11:20:00"
    teststring = "xxx"
    testnumber = 123.123
    testarray1 = np.array([1.23,23.45,np.nan,2.45])
    testarray2 = np.array([datetime(2024,11,22,5),datetime(2024,11,22,6),"",datetime(2024,11,22,9)])
    testarray3 = np.array([datetime(2024,11,22,5),datetime(2024,11,22,6),datetime(2024,11,22,9),datetime(2024,11,22,11)])
    try:
        var1 = is_number(teststring)
        var2 = is_number(testnumber)
    except Exception as excep:
        errors['is_number'] = str(excep)
        print(datetime.utcnow(), "--- ERROR testing number.")

    try:
        var1 = testtime(testdate)
    except Exception as excep:
        errors['testdate'] = str(excep)
        print(datetime.utcnow(), "--- ERROR testdate.")

    try:
        ar1 = maskNAN(testarray1)
        ar2 = maskNAN(testarray2)
    except Exception as excep:
        errors['mask_nan'] = str(excep)
        print(datetime.utcnow(), "--- ERROR maskNAN.")
    print()
    print("----------------------------------------------------------")
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(str(errors.keys()))
        print()
        print("Exceptions thrown:")
        for item in errors:
            print ("{} : errormessage = {}".format(item, errors.get(item)))

