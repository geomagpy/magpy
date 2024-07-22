# -*- coding: utf-8 -*-

"""
methods contain all methods which do not fit to a magpy specific class

the following methods are contained:
- is_number(variable)    :    returns True if variable is float or int
- ceil_dt(datetime,seconds):  will round datetime towards the next time step defined by seconds
- testtime(variable)     :    returns datetime object if variable can be converted to it
- mask_nan(array)        :    returns an array without nan or empty elements
- missingvalue()         :    will replace nan vaules in array with means, interpolation or given fill values

"""
#from magpy.stream import * # os, num2date
import numpy as np
from datetime import datetime, timedelta
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


def ceil_dt(dt,seconds):
    """
    DESCRIPTION:
        Function to round time to the next time step as given by its seconds
        minute: 60 sec
        quater hour: 900 sec
        hour:   3600 sec
    PARAMETER:
        dt: (datetime object)
        seconds: (integer)
    USAGE:
        >>>print ceil_dt(datetime(2014,01,01,14,12,04),60)
        >>>2014-01-01 14:13:00
        >>>print ceil_dt(datetime(2014,01,01,14,12,04),3600)
        >>>2014-01-01 15:00:00
        >>>print ceil_dt(datetime(2014,01,01,14,7,0),60)
        >>>2014-01-01 14:07:00
    """
    #how many secs have passed this hour
    nsecs = dt.minute*60+dt.second+dt.microsecond*1e-6
    if nsecs % seconds:
        delta = (nsecs//seconds)*seconds+seconds-nsecs
        return dt + timedelta(seconds=delta)
    else:
        return dt


def find_nearest(array,value):
    """
    Find the nearest element within an array
    """
    array = np.ma.masked_invalid(array)
    idx = (np.abs(array-value)).argmin()
    return array[idx], idx


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

def missingvalue(v,window_len=60,threshold=0.9,fill='mean',fillvalue=99999):
        """
        DESCRIPTION
            Fills missing values (np.nan) either with means, interpolated values
            or a given fillvalue.
            This method is used self.filter (interpolate and/or mean)
            and can be used to replace np.nan values by typical IM plasecholders
        PARAMETER:
            v: 			   (np.array) single column of ndarray
            window_len:    (int) length of window to check threshold
            threshold: 	   (float) minimum percentage of available data e.g. 0.9 - 90 precent
            fill: 	       (string) 'mean', 'interpolation' or 'value'
            fillvalue: 	   (float) if fill by value then this value will be used
        RETURNS:
            ndarray - single column
        """
        if fill in ['mean','interpolate','interpolation']:
            try:
                v_rest = np.array([])
                v = v.astype(float)
                n_split = len(v)/float(window_len)
                if not n_split == int(n_split):
                    el = int(int(n_split)*window_len)
                    v_rest = v[el:]
                    v = v[:el]
                spli = np.split(v,int(len(v)/window_len))
                if len(v_rest) > 0:
                    spli.append(v_rest)
                newar = np.array([])
                for idx,ar in enumerate(spli):
                    nans, x = nan_helper(ar)
                    if len(ar[~nans]) >= threshold*len(ar):
                        if fill == 'mean':
                            ar[nans]= np.nanmean(ar)
                        else:
                            ar[nans]= np.interp(x(nans), x(~nans), ar[~nans])
                    newar = np.concatenate((newar,ar))
                v = newar
            except:
                print ("Filter: could not split stream in equal parts for interpolation - switching to conservative mode")
        elif fill in ['value']:
            naninds = np.argwhere(np.isnan(v))
            v[naninds]= fillvalue

        return v

def nan_helper(y):
    """Helper to handle indices and logical indices of NaNs. Taken from eat (http://stackoverflow.com/questions/6518811/interpolate-nan-values-in-a-numpy-array)

    Input:
        - y, 1d numpy array with possible NaNs
    Output:
        - nans, logical indices of NaNs
        - index, a function, with signature indices= index(logical_indices),
         to convert logical indices of NaNs to 'equivalent' indices
    Example:
        >>> # linear interpolation of NaNs
        >>> nans, x= nan_helper(y)
        >>> y[nans]= np.interp(x(nans), x(~nans), y[~nans])
    """
    y = np.asarray(y).astype(float)
    return np.isnan(y), lambda z: z.nonzero()[0]


def nearestPow2(x):
    """
    Function taken from ObsPy
    Find power of two nearest to x
    >>> nearestPow2(3)
    2.0
    >>> nearestPow2(15)
    16.0
    :type x: Float
    :param x: Number
    :rtype: Int
    :return: Nearest power of 2 to x
    """

    a = pow(2, ceil(np.log2(x)))
    b = pow(2, floor(np.log2(x)))
    if abs(a - x) < abs(b - x):
        return a
    else:
        return b


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


def test_timestring(time):
    """
    Check the date/time input and returns a datetime object if valid:

    ! Use UTC times !

    - accepted are the following inputs:
    1) absolute time: as provided by date2num
    2) strings: 2011-11-22 or 2011-11-22T11:11:00
    3) datetime objects by datetime.datetime e.g. (datetime(2011,11,22,11,11,00)
    """

    timeformats = ["%Y-%m-%d",
                   "%Y-%m-%dT%H:%M:%S",
                   "%Y-%m-%d %H:%M:%S.%f",
                   "%Y-%m-%dT%H:%M:%S.%f",
                   "%Y-%m-%d %H:%M:%S",
                   "%Y-%m-%dT%H:%M:%SZ"
                   ]

    basestring = (str, bytes)

    if isinstance(time, float) or isinstance(time, int):
        try:
            timeobj = num2date(time).replace(tzinfo=None)
        except:
            raise TypeError
    elif isinstance(time, basestring): # test for str only in Python 3 should be basestring for 2.x
        for i, tf in enumerate(timeformats):
            try:
                timeobj = datetime.strptime(time,tf)
                break
            except:
                pass
    elif isinstance(time, str): # test for str only in Python 3 should be basestring for 2.x
        for i, tf in enumerate(timeformats):
            try:
                timeobj = datetime.strptime(time,tf)
                break
            except:
                j = i+1
                pass
        if j == len(timeformats):     # Loop found no matching format
            try:
                # Necessary to deal with old 1000000 micro second bug
                timearray = time.split('.')
                print(timearray)
                if len(timearray) > 1:
                    if timearray[1] == '1000000':
                        timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")+timedelta(seconds=1)
                    else:
                        # This would be wrong but leads always to a TypeError
                        timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")
            except:
                raise TypeError
    elif not isinstance(time, datetime):
        raise TypeError
    else:
        timeobj = time

    return timeobj



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
    v = np.array([1, 2, 3, 4, 5, np.nan, 7, 8, 9, 10, 11, 12, 13, 14])
    try:
        var1 = is_number(teststring)
        var2 = is_number(testnumber)
    except Exception as excep:
        errors['is_number'] = str(excep)
        print(datetime.utcnow(), "--- ERROR testing number.")

    try:
        var1 = testtime(testdate)
        print (var1)
        var2 = ceil_dt(var1,3600)
        print ("Rounded to hour by ceil_dt", var2)
    except Exception as excep:
        errors['testdate'] = str(excep)
        print(datetime.utcnow(), "--- ERROR testdate.")

    try:
        for fill in ['mean','interpolate','value']:
            mv = missingvalue(v,window_len=10,fill=fill, fillvalue=99)
            print ("filling option {}: {}".format(fill,mv))
    except Exception as excep:
        errors['missingvalue'] = str(excep)
        print(datetime.utcnow(), "--- ERROR with missingvalue.")

    try:
        a,b = find_nearest(v,10.3)
        print (a,b)
    except Exception as excep:
        errors['find_nearest'] = str(excep)
        print(datetime.utcnow(), "--- ERROR with find_nearest.")
    try:
        a = test_timestring(testdate)
    except Exception as excep:
        errors['test_timestring'] = str(excep)
        print(datetime.utcnow(), "--- ERROR with test_timestring.")
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

