# -*- coding: utf-8 -*-

"""
methods contain all methods which do not fit to a magpy specific class

the following methods are contained:
- is_number(variable)    :    returns True if variable is float or int
- testtime(variable)     :    returns datetime object if variable can be converted to it

"""
from magpy.stream import * # os, num2date


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
        elif isinstance(time, datetime64):
            unix_epoch = np.datetime64(0, 's')
            one_second = np.timedelta64(1, 's')
            seconds_since_epoch = (time - unix_epoch) / one_second
            timeobj = datetime.utcfromtimestamp(seconds_since_epoch)
        elif not isinstance(time, datetime):
            raise TypeError
        else:
            timeobj = time

        return timeobj
