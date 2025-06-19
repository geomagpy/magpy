# -*- coding: utf-8 -*-

"""
methods contain all methods which do not fit to a magpy specific class

the following methods are contained:
- ceil_dt(datetime,seconds)  :  will round datetime towards the next time step defined by seconds
- convert_geo_coordinate(lon,lat,pro1,pro2)  :  converts geographic coordinates based on EPSG codes
- data_for_di(many)  :  load datastream and apply multiple corrections and rotations for DI analysis
- deprecated(reason)  :   create decrepated messages
- denormalize - to be removed
- dictgetlast()  : get last value of typical old dict structure
- dict2string()  : convert a dict to string for storage in db table for faster access
- evaluate_function(component, function, samplingrate, starttime=None, endtime=None, debug=False)
- extract_date_from_string(datestring)
- find_nearby(array, value)
- find_nth(string, substring, nth occurrence)   : returns index of nth occurrence of substring in string
- func_from_file(functionpath,debug=False)   :    read functional parameters from file
- func_to_file(funcparameter,functionpath,debug=False)  :    read function parameters (NOT the function) to file
- group_indices(indexlist)   :  identify successiv indices and return a list with start,end pairs
- is_number(variable)    :    returns True if variable is float or int
- mask_nan(array)        :    returns an array without nan or empty elements
- missingvalue()         :    will replace nan vaules in array with means, interpolation or given fill values
- nan_helper(y)
- nearestpow2
- normalize
- round_second(datettime) :  rounds the given datetime to its next second
- string2dict()  : convert a string to dict for storage in db table for faster access
- testtime(variable)     :    returns datetime object if variable can be converted to it
- test_timestring(variable)     :

|class | method | since version | until version | runtime test | result verification | manual | *tested by |
|----- | ------ | ------------- | ------------- | ------------ | ------------------- | ------ | ---------- |
|**core.methods** |  |          |               |              |  |  | |
|    | ceil_dt         |  2.0.0 |              | yes           | yes          |        | |
|    | convert_geo_coordinate | 2.0.0 |        | yes           | yes          |        | |
|    | dates_to_url    | 2.0.0 |               |               | yes          |        | |
|    | deprecated      | 2.0.0 |               | --            | --           |        | |
|    | denormalize     | 2.0.0 |               | no            | yes*         |        | MARTAS baseline|
|    | dictdiff        | 2.0.0 |               | yes           | yes          |        | |
|    | dictgetlast     | 2.0.0 |               | yes           | yes          |        | |
|    | dict2string     | 2.0.0 |               | yes           | yes          |        | |
|    | evaluate_function | 2.0.0 |             |               | yes*         |            |  plot |
|    | extract_date_from_string | 2.0.0 |      | yes           | yes          |        | |
|    | find_nearest    | 2.0.0 |               | yes           | yes          |  | |
|    | find_nth        | 2.0.0 |               | yes           | yes          |            |  flagbrain |
|    | func_from_file  | 2.0.0 |               | yes           | yes*         |  5.9       |  stream |
|    | func_to_file    | 2.0.0 |               | yes           | yes*         |  5.9       |  stream |
|    | get_chunks      | 2.0.0 |               | yes           | yes          |        | |
|    | group_indices   | 2.0.0 |               | yes           | yes          |        | |
|    | is_number       | 2.0.0 |               | yes           | yes          |        | |
|    | mask_nan        | 2.0.0 | maskNAN       | yes           | yes          |        | |
|    | missingvalue    | 2.0.0 |               | yes           | yes          |        | |
|    | nan_helper      | 2.0.0 |               | yes           | yes          |        | |
|    | nearestpow2     | 2.0.0 |               | yes           | yes          |        | |
|    | normalize       | 2.0.0 |               |               | yes          |        | |
|    | round_second    | 2.0.0 |               | yes           | yes          |        | |
|    | string2dict     | 2.0.0 |               | yes           | yes          |  | |
|    | testtime        | 2.0.0 |               | yes           | yes*         |            |  library |
|    | test_timestring | 2.0.0 |               |               | yes          |        | |

"""
import numpy as np
from datetime import datetime, timedelta, timezone
from matplotlib.dates import num2date, date2num
import os
import re
import dateutil.parser as dparser
import functools
import inspect
import warnings
import json

# import pyproj  # convertGeoCoor


def ceil_dt(dt, seconds):
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
        print (ceil_dt(datetime(2014,1,1,14,12,4), 60))
        2014-01-01 14:13:00
        print (ceil_dt(datetime(2014,1,1,14,12,4), 3600))
        2014-01-01 15:00:00
        $ print ceil_dt(datetime(2014,01,01,14,7,0),60)
        $ 2014-01-01 14:07:00
    """
    nsecs = dt.minute*60+dt.second+dt.microsecond*1e-6
    if nsecs % seconds:
        delta = (nsecs//seconds)*seconds+seconds-nsecs
        return dt + timedelta(seconds=delta)
    else:
        return dt


def convert_geo_coordinate(lon,lat,pro1,pro2):
    """
    DESCRIPTION:
       converts longitude latitude using the provided epsg codes
    PARAMETER:
       lon	(float) longitude
       lat	(float) latitude
       pro1	(string) epsg code for source ('epsg:32909')
       pro2	(string) epsg code for output ('epsg:4326')
    RETURNS:
       lon, lat	(floats) longitude,latitude
    APLLICATION:
       lon, lat = convert_geo_coordinate(float(longi),float(lati),'epsg:31256','epsg:4326')
    USED BY:
       writeIMAGCDF,writeIAGA,writeIAF
    """
    x1 = float(lon)
    y1 = float(lat)
    try:
        from pyproj import Proj
        p1 = Proj(pro1)
        p2 = Proj(pro2)
    except:
        print ("convert_geo_coordinate: problem (import pyproj or epsg code error)")
        return lon, lat

    try:
        # pyproj2
        from pyproj import Transformer
        transformer = Transformer.from_crs(pro1.upper(), pro2.upper())
        y2, x2 = transformer.transform(y1, x1)
        return x2, y2
    except:
        # pyproj1
        from pyproj import Proj, transform
        try:
            p1 = Proj(pro1)
        except:
            p1 = Proj(init=pro1)
        # projection 2: WGS 84
        try:
            p2 = Proj(pro2)
        except:
            p2 = Proj(init=pro2)
        # transform this point to projection 2 coordinates.
        x2, y2 = transform(p1,p2,x1,y1,always_xy=True)
        return x2, y2


def dates_to_url(url, starttime=None, endtime=None, starttimestring='starttime', endtimestring='endtime', debug=False):
    """
    DESCRIPTION
        check if provided string is url and if yes  then eventually fill in starttime and endtime
        into the url (if nor already contained)
    :param url:
    :param starttime:
    :param endtime:
    :param debug:
    :return:
    """
    if "://" in url:
        if debug:
            print (" url before dates_to_url", url)
        dformat = "%Y-%m-%dT%H:%M:%SZ"
        if "Date" in starttimestring or "Date" in endtimestring:
            dformat = "%Y-%m-%d"
        if starttime:
            indst = url.find(starttimestring)
            if indst < 0:
                st = datetime.strftime(starttime,dformat)
                url = "{}&{}={}".format(url,starttimestring,st)
        if endtime:
            indet = url.find(endtimestring)
            if indet < 0:
                et = datetime.strftime(endtime, dformat)
                url = "{}&{}={}".format(url, endtimestring, et)
        if debug:
            print(" url after dates_to_url", url)
    return url


def deprecated(reason):
    """
    This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used.
    copy/paste from

    """
    string_types = (type(b''), type(u''))

    if isinstance(reason, string_types):

        # The @deprecated is used with a 'reason'.
        #
        # .. code-block:: python
        #
        #    @deprecated("please, use another function")
        #    def old_function(x, y):
        #      pass

        def decorator(func1):

            if inspect.isclass(func1):
                fmt1 = "Call to deprecated class {name} ({reason})."
            else:
                fmt1 = "Call to deprecated function {name} ({reason})."

            @functools.wraps(func1)
            def new_func1(*args, **kwargs):
                warnings.simplefilter('always', DeprecationWarning)
                warnings.warn(
                    fmt1.format(name=func1.__name__, reason=reason),
                    category=DeprecationWarning,
                    stacklevel=2
                )
                warnings.simplefilter('default', DeprecationWarning)
                return func1(*args, **kwargs)

            return new_func1

        return decorator

    elif inspect.isclass(reason) or inspect.isfunction(reason):

        # The @deprecated is used without any 'reason'.
        #
        # .. code-block:: python
        #
        #    @deprecated
        #    def old_function(x, y):
        #      pass

        func2 = reason

        if inspect.isclass(func2):
            fmt2 = "Call to deprecated class {name}."
        else:
            fmt2 = "Call to deprecated function {name}."

        @functools.wraps(func2)
        def new_func2(*args, **kwargs):
            warnings.simplefilter('always', DeprecationWarning)
            warnings.warn(
                fmt2.format(name=func2.__name__),
                category=DeprecationWarning,
                stacklevel=2
            )
            warnings.simplefilter('default', DeprecationWarning)
            return func2(*args, **kwargs)

        return new_func2

    else:
        raise TypeError(repr(type(reason)))


def denormalize(column, startvalue, endvalue):
    """
    DESCRIPTION:
        used only by MARTAS - baseline_overview method
    converts [0:1] back with given start and endvalue
    """
    normcol = []
    if startvalue>0:
        if endvalue < startvalue:
            raise ValueError("start and endval must be given, endval must be larger")
        else:
            for elem in column:
                normcol.append((elem*(endvalue-startvalue)) + startvalue)
    else:
        raise ValueError("start and endval must be given as absolute times")

    return normcol


def dict2string(dictionary, typ='dictionary'):
        """
        DEFINITION:
            converts strings (as taken from a database) to a dictionary or a list of dictionaries

        VARIABLES:
            dictionary    :    dictionary
            typ           :    dictionary, listofdict, array
        """
        string = "{}".format(dictionary).replace("u'", "'")
        if typ == 'dictionary':
            string1 = string.replace(' ', '').replace("':'", "_").replace("{", "(").replace("}", ")")
            string2 = string1.replace("':('", "_(").replace("'),'", ");").replace("','", ";").replace("')),'",
                                                                                                      "));").replace(
                "'", "")[1:-1]
            return string2
        elif typ == 'listofdict':
            string1 = string.replace(' ', '').replace("':'", "_").replace("{", "(").replace("}", ")")
            string2 = string1.replace("'", "")[1:-1]
            return string2


def dictdiff(dict_a, dict_b, show_value_diff=True):
    """
    DESCRIPTION
        Compares two dictionaries. Useful for header analysis.
        Code was taken as is from
        https://stackoverflow.com/questions/32815640/how-to-get-the-difference-between-two-dictionaries-in-python
        and written by juandesant.
    """
    result = {}
    result['added']   = {k: dict_b[k] for k in set(dict_b) - set(dict_a)}
    result['removed'] = {k: dict_a[k] for k in set(dict_a) - set(dict_b)}
    if show_value_diff:
        common_keys =  set(dict_a) & set(dict_b)
        result['value_diffs'] = {
            k:(dict_a[k], dict_b[k])
            for k in common_keys
            if dict_a[k] != dict_b[k]
        }
    return result


def dicgetlast(dictionary, pier=None, element=None):
    """
    DEFINITION:
        get last delta value inputs from a dictionary with year keys
    RETURN:
        Returns a value dictionary
    APPLICTAION:
        result = dicgetlast(dictionary,pier='A2',element='deltaD,deltaI,deltaF')
    EXAMPLE:
    """
    returndic = {}
    if pier:
        testdic = dictionary[pier]  # append new values here (a2dic[year] = newvaluedict; dic['A2'] = a2dic)
    else:
        testdic = dictionary  # append new values here (a2dic[year] = newvaluedict; dic['A2'] = a2dic)
    if not element:
        years = [int(ye) for ye in testdic]
        value = testdic.get(str(max(years)))
        returndic[str(max(years))] = value
    else:
        # get last year for each value
        listelement = element.split(',')
        existdelta = []
        for elem in ['deltaD', 'deltaI', 'deltaF']:
            # get years when elem was determined
            years = [int(ye) for ye in testdic if not testdic[ye].get(elem, '') == '']
            if len(years) > 0:
                value = testdic.get(str(max(years))).get(elem, '')
                returndic[elem] = value
    return returndic


def extract_date_from_string(datestring):
    """
    DESCRIPTION:
       Method to identify a date within a string (usually the filename).
       It is used by most file reading procedures
    RETURNS:
       A list of datetimeobjects with first and last date (month, year)
       or the day (dailyfiles)
    APPLICATION:
       datelist = extract_date_from_string(filename)
    EXAMPLES:
       d = extract_date_from_string('gddtw_2022-11-22.txt')
       d = extract_date_from_string('2022_gddtw.txt')
    """

    date = False
    # get day from filename (platform independent)
    localechanged = False

    try:
        splitpath = os.path.split(datestring)
        daystring = splitpath[1].split('.')[0]
    except:
        daystring = datestring

    try:
        tmpdaystring = daystring[-7:]
        if not tmpdaystring.isdigit() and tmpdaystring[-4:].isdigit():
            # if not only numbers then check for something like jan0523
            date = dparser.parse(tmpdaystring[:5]+' '+tmpdaystring[5:], dayfirst=True)
            dateform = '%b%d%y'
        else:
            x=1/0 # break it
    except:
        # test for day month year
        try:
            tmpdaystring = re.findall(r'\d+',daystring)[0]
        except:
            # no number whatsoever
            return False
        testunder = daystring.replace('-','').split('_')
        for i in range(len(testunder)):
            try:
                numberstr = re.findall(r'\d+',testunder[i])[0]
            except:
                numberstr = '0'
            if len(numberstr) > 4 and int(numberstr) > 100000: # There needs to be year and month
                tmpdaystring = numberstr
            elif len(numberstr) == 4 and int(numberstr) > 1900: # use year at the end of string
                tmpdaystring = numberstr

        if len(tmpdaystring) > 8:
            try: # first try whether an easy pattern can be found e.g. test12014-11-22
                match = re.search(r'\d{4}-\d{2}-\d{2}', daystring)
                date = datetime.strptime(match.group(), '%Y-%m-%d').date()
            except:  # if not use the first 8 digits
                tmpdaystring = tmpdaystring[:8]
                pass
        if len(tmpdaystring) == 8:
            try:
                dateform = '%Y%m%d'
                date = datetime.strptime(tmpdaystring,dateform)
            except:
                # log ('dateformat in filename could not be identified')
                pass
        elif len(tmpdaystring) == 6:
            try:
                dateform = '%Y%m'
                date = datetime.strptime(tmpdaystring,dateform)
                from calendar import monthrange
                datelist = [datetime.date(date), datetime.date(date + timedelta(days=monthrange(date.year,date.month)[1]-1))]
                return datelist
            except:
                # log ('dateformat in filename could not be identified')
                pass
        elif len(tmpdaystring) == 4:
            try:
                dateform = '%Y'
                date = datetime.strptime(tmpdaystring,dateform)
                date2 = datetime.strptime(str(int(tmpdaystring)+1),dateform)
                datelist = [datetime.date(date), datetime.date(date2-timedelta(days=1))]
                return datelist
            except:
                # log ('dateformat in filename could not be identified')
                pass

        if not date and len(daystring.split('_')[0]) > 8:
            try: # first try whether an easy pattern can be found e.g. test12014-11-22_00-00-00
                daystrpart = daystring.split('_')[0] # e.g. RCS
                match = re.search(r'\d{4}-\d{2}-\d{2}', daystrpart)
                date = datetime.strptime(match.group(), '%Y-%m-%d').date()
                return [date]
            except:
                pass

        if not date:
            # No Date found so far - now try last 6 elements of string () e.g. SG gravity files
            try:
                tmpdaystring = re.findall(r'\d+',daystring)[0]
                dateform = '%y%m%d'
                date = datetime.strptime(tmpdaystring[-6:],dateform)
            except:
                pass
    try:
        return [datetime.date(date)]
    except:
        return [date]


def evaluate_function(component, function, samplingrate, starttime=None, endtime=None, debug=False):
    """
    DESCRIPTION
        Evaluates a function obtained i.e. by the fit method and returns the times and function values
        within the selected time range.
        Please note: function values outside the originally fitted datapoints will be np.nan. If you
        want to get fits for ranges outside the data range use stream.extrapolate first
    PARAMETERS:
        starttime and endtime defines the range in which function is evaluated
        functiontimes func[1] and func[2] defines range in which func was determined
    APPLICATION:
        used by core.plot

    :param component:  key of the function value (i.e. x for fx)
    :param function:   function as obtained by the stream.fit() method
    :param samplingrate:   samplingrate es obtained by stream.samplingrate()
    :param starttime:  optional - evaluation begin
    :param endtime:   optional - evaluation end
    :param debug:
    :return: (list) containing [array(times), array(values)]

    """
    if debug:
        print("Function looks like:", function)
    func = function[0].get("f{}".format(component))
    if not func:
        if debug:
            print ("did not find a function reference to this component")
        return None
    if starttime:
        sttime = np.datetime64(testtime(starttime))
    else:
        sttime = np.datetime64(testtime(function[-3]))
    if endtime:
        entime = np.datetime64(testtime(endtime))
    else:
        entime = np.datetime64(testtime(function[-2]))
    tfunc1 = np.datetime64(testtime(function[1]))
    tfunc2 = np.datetime64(testtime(function[2]))
    samprate = samplingrate * 1000000.
    # do the following with projected date ranges
    ftime = np.arange(sttime, entime + np.timedelta64(int(samprate), "us"), np.timedelta64(int(samprate), "us"))
    # function was determined between 0=function[1] and 1=function[2]
    # obtain a normalized new range:
    if debug:
        print(tfunc1, tfunc2, 0, 1, sttime)

    def _get_y(x1, x2, y1, y2, x3):
        x1 = x1.astype(np.float64)
        x2 = x2.astype(np.float64)
        x3 = x3.astype(np.float64)
        return (y2 - y1) / (x2 - x1) * (x3 - x2) + y2

    low = _get_y(tfunc1, tfunc2, 0, 1, sttime)
    high = _get_y(tfunc1, tfunc2, 0, 1, entime)
    if debug:
        print(low, high)
    newt = np.linspace(low, high, len(ftime))
    if debug:
        print(len(ftime), len(newt))
    fvals = func(newt)
    if debug:
        print(ftime, fvals)
    return [ftime, fvals]


def find_nearest(array, value):
    """
    DESCRIPTION
        Find the nearest element within an array
    APPLICATION
        array_value, array_idx = find_nearest(array, value)
    """
    if not isinstance(value,(datetime, np.datetime64)):
        array = np.ma.masked_invalid(array)
    idx = (np.abs(array-value)).argmin()
    return array[idx], idx


def find_nth(haystack: str, needle: str, n: int) -> int:
    """
    DESCRIPTION
        Find the nth occurrence of a substring in a string
        #https://stackoverflow.com/questions/1883980/find-the-nth-occurrence-of-substring-in-a-string
    """
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start


def func_from_file(functionpath,debug=False):
        """
        DESCRIPTION
            Load function parameters from file
        """
        fitparameters = {}
        try:
            if debug:
                print ("Reading a json style fit parameter list...")
            def dateparser(dct):
                # Convert dates in dictionary to datetime objects
                for (key,value) in dct.items():
                    try:
                        if isinstance(value, (list,tuple)):
                            value = value
                        else:
                            value = float(value)
                    except:
                        try:
                            value = str(value)
                            if str(value).count('-') + str(value).count(':') == 4:
                                try:
                                    try:
                                        value = datetime.strptime(value,"%Y-%m-%d %H:%M:%S.%f")
                                    except:
                                        value = datetime.strptime(value,"%Y-%m-%d %H:%M:%S")
                                except:
                                    pass
                            elif value.startswith("now"):
                                tst=value.split("+")
                                if len(tst)>1 and isinstance(tst[1],int):
                                    value = datetime.now(timezone.utc).replace(tzinfo=None)+timedelta(days=int(tst[1]))
                                else:
                                    value = datetime.now(timezone.utc).replace(tzinfo=None)
                        except:
                            pass
                    dct[key] = value
                return dct

            if os.path.isfile(functionpath):
                with open(functionpath,'r') as file:
                    fitparameters = json.load(file)
                    #fitparameters = json.load(file,object_hook=dateparser)
                for key in fitparameters:
                    value = fitparameters[key]
                    key = int(key)
                    value = dateparser(value)
                if debug:
                    print (" -> success", fitparameters)
            else:
                if debug:
                    print ("Fit parameter file not existing ...")
        except:
            if debug:
                print ("Loading fit parameter - general error")

        return fitparameters

def func_to_file(funcparameter,functionpath,debug=False):
        """
        DESCRIPTION
            Save function to file
        """
        def dateconv(d):
            # Converter to serialize datetime objects in json
            if isinstance(d,datetime):
                return d.__str__()

        if debug:
            print ("func_to_file: writing function data to file")
        if isinstance(funcparameter, dict):
            if debug:
                print ("Found dictionary")
            funcres = funcparameter
        else:
            if isinstance(funcparameter[0], dict):
                funct = [funcparameter]
            else:
                funct = funcparameter
            if debug:
                print ("Found list/single function", funct)
            funcres = {}
            for idx, func in enumerate(funct):
                if debug:
                    print ("funcs", func)
                #func = [functionkeylist, sv, ev, fitfunc, fitdegree, knotstep, starttime, endtime]
                if len(func) >= 9:
                    funcdict = {"keys":func[8], "fitfunc":func[3],"fitdegree":func[4], "knotstep":func[5], "starttime":func[6],"endtime":func[7], "functionlist":"dropped", "sv":func[1], "ev":func[2]}
                    funcres[idx] = funcdict
            if debug:
                print (funcres)
        #convert date times and remove function object
        try:
            with open(functionpath, 'w', encoding='utf-8') as f:
                json.dump(funcres, f, ensure_ascii=False, indent=4, default=dateconv)
        except:
            return False
        return True


def get_chunks(endchunk, wl=3600):
    """
    DESCRIPTION
       get the distribution and lengths of time windows between potentially disturbed time ranges.
       this statistic will help to identify i.e. lightning strikes, eventually vehicles if passing and coming back.
       do this analysis time dependent: run a gliding window of default 2h duration across the sequence.
       create 2h/2 overlapping 2h window with some characteristics on average window distances and amount
    PARAMETER
       endchunk : the length of the list
       wl : half window length - half of the window length and overlap between different windows
    APPLICATION
       l = get_chunks(10800, wl=3600)

    :param endchunk:
    :param wl:
    :return:
    """

    chunks = []
    startchunk = 0
    for i in range(startchunk, endchunk, wl):
        x = i
        if x+ 2 * wl <= endchunk:
            chunks.append(range(x, x + 2 * wl))
    return chunks


def group_indices(indexlist):
    """
    DESCRIPTION
        group successive indices in list with start and endindex
        This method is useful for creating flagging structures
    APPLICATION
        used by merge_stream to identify ranges which are inserted
    """
    flip = np.diff((np.diff(indexlist) == 1) + 0, prepend=0, append=0)
    single = []
    startcount = True
    for i,el in enumerate(flip):
        if el == -1:
            #start counting zeros
            startcount = True
        if el == 0 and startcount:
            single.append([i,i])
        if el == 1:
            startcount = False
    # Look for where it flips from 1 to 0, or 0 to 1.
    start_idx = np.where(flip == 1)
    end_idx = np.where(flip == -1)
    for i,el in enumerate(start_idx[0]):
        single.append([el,end_idx[0][i]])
    return single


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


def mask_nan(column):
    return maskNAN(column)


def maskNAN(column):
    """
    Tests for NAN values in column and usually masks them
    """

    numeric = False
    datetype = False
    num_found = False

    if len(column) > 0:
        if is_number(column[0]):
            numeric = True
        elif isinstance(column[0], (datetime, np.datetime64)):
            datetype = True
    else:
        return column

    if numeric:
        try:  # Test for the presence of nan values to it
            column = np.asarray(column).astype(float)
            val = np.mean(column)
            if np.isnan(val):  # found at least one nan value
                for el in column:
                    if not np.isnan(el):  # at least on number is present - use masked_array
                        num_found = True
                if num_found:
                    mcolumn = np.ma.masked_invalid(column)
                    column = mcolumn
                else:
                    return []
        except:
            return []
    elif datetype:
        try:
            indicies = np.argwhere(column == "")
            mask = np.zeros(len(column), dtype=bool)
            if indicies.size > 0:
                mask[indicies[0]] = True
                column = column[~mask]
        except:
            pass
    else:
        try:
            column = np.ma.masked_where(column == "", column)
        except:
            pass

    return column


def missingvalue(v, window_len=60, threshold=0.9, fill='mean', fillvalue=99999):
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
    if fill in ['mean', 'interpolate', 'interpolation']:
        try:
            v_rest = np.array([])
            v = v.astype(float)
            n_split = len(v)/float(window_len)
            if not n_split == int(n_split):
                el = int(int(n_split)*window_len)
                v_rest = v[el:]
                v = v[:el]
            spli = np.split(v, int(len(v)/window_len))
            if len(v_rest) > 0:
                spli.append(v_rest)
            newar = np.array([])
            for idx, ar in enumerate(spli):
                nans, x = nan_helper(ar)
                if len(ar[~nans]) >= threshold*len(ar):
                    if fill == 'mean':
                        ar[nans] = np.nanmean(ar)
                    else:
                        ar[nans] = np.interp(x(nans), x(~nans), ar[~nans])
                newar = np.concatenate((newar, ar))
            v = newar
        except:
            print("Filter: could not split stream in equal parts for interpolation - switching to conservative mode")
    elif fill in ['value']:
        naninds = np.argwhere(np.isnan(v))
        v[naninds] = fillvalue

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
        # linear interpolation of NaNs
        nans, x= nan_helper(y)
        y[nans]= np.interp(x(nans), x(~nans), y[~nans])
    """
    y = np.asarray(y).astype(float)
    return np.isnan(y), lambda z: z.nonzero()[0]


def nearestPow2(x):
    """
    DESCRIPTION
        Find power of two nearest to x
    APPLICATION
        nearestPow2(3)
        -> 2.0
        nearestPow2(15)
        -> 16.0
    :type x: Float
    :param x: Number
    :rtype: Int
    :return: Nearest power of 2 to x
    """

    a = pow(2, np.ceil(np.log2(x)))
    b = pow(2, np.floor(np.log2(x)))
    if abs(a - x) < abs(b - x):
        return a
    else:
        return b


def normalize(column):
    """
    normalizes the given column to range [0:1]
    """
    normcol = []
    timeconv = False
    # test column contents
    if isinstance(column[0], (float,int,np.float64)):
        column = column.astype(float)
    elif isinstance(column[0], (datetime,np.datetime64)):
        column = date2num(column)
        timeconv = True
    else:
        print ("stream._normalize: column does not contain numbers")
    maxval = np.max(column)
    minval = np.min(column)
    for elem in column:
        normcol.append((elem-minval)/(maxval-minval))

    return normcol, minval, maxval


def round_second(obj: datetime) -> datetime:
    if obj.microsecond >= 500_000:
        obj += timedelta(seconds=1)
    return obj.replace(microsecond=0)


def string2dict(string, typ='dictionary'):
    """
    DEFINITION:
        converts strings (as taken from a database) to a dictionary or a list of dictionaries

    VARIABLES:
        string    :    a string like:
        typ       :    dictionary, listofdict, array
    # The following convention should apply:
    # ',' separates list element belonging to a certain key -> []
    # ';' splits dictionary inputs like {x:y,z:w} -> ','
    # '_' separtes key and value -> :
    # '(' defines dictionary input -> { (})

    EXAMPLES:
        A) dictionary
         string2dict('A2_(2017_(deltaD_0.00;deltaI_0.201;deltaF_1.12);2018_(deltaF_1.11))')
         string2dict('data_(x_[1,2,3,4,5];y_[3,2,1,4,5];z_[4,5,6,7,6])')
         string2dict('2018_0.532')
         string2dict('2016_0.532;2017_0.231;2018_0.123')
        B) listofdict
         string2dict('2016_0.532,2017_0.231,2018_0.123',typ='listofdict')
         string2dict('st_736677.0,time_timedelta(seconds=-2.3),et_736846.0',typ='listofdict'))
         string2dict('st_719853.0,f_-1.48,time_timedelta(seconds=-3.0),et_736695.0;st_736695.0,f_-1.57,
         time_timedelta(seconds=-3.0), et_736951.5;st_736951.5,f_-1.57,time_timedelta(seconds=1.50),et_737060.0;
         st_737060.0,f_-1.57,time_timedelta(seconds=-0.55)',typ='listofdict')
        C) array
         string2dict('2,3,4,5,8;1,2,3,4,5;8,5,6,7,8',typ='array')
        D) olddeltadict (too be removed)
         string2dict('A2_2015_0.00_0.00_201510_-0.13,A2_2016_0.00_0.00_201610_-0.06,A2_2017_0.00_0.00_201707_-0.03',
         typ='olddeltadict')

    APPLICTAION:
         st = 'A2_(2017_(deltaD_0.00;deltaI_0.201;deltaF_1.12);2018_(deltaF_1.11));A3_(2018_(deltaF_3.43))'
         dic = string2dict(st)
         print (dic['A2']['2018'])

    """
    string = string.replace("\r", "").replace("\n", "").replace(" ", "")

    if typ == 'dictionary':
        dic = "{}".format(
            string.replace("(", "{\"").replace(")", "\"}").replace("_", "\":\"").replace(";", "\",\""))
        dic2 = "{\"" + "{}".format(
            dic.replace(":\"{", ":{").replace("}\"", "}").replace("\"[", "[").replace("]\"", "]"))
        if dic2.endswith("}") or dic2.endswith("]"):
            dic3 = dic2 + "}"
        else:
            dic3 = dic2 + "\"}"
        return eval(dic3)
    elif typ == 'listofdict':
        array = []
        liste = string.split(';')
        for el in liste:
            line = el.split(',')
            dic = {}
            for elem in line:
                if not elem.find('_') > 0:
                    print("Wrong type")
                dic[elem.split('_')[0].strip()] = elem.split('_')[1].strip()
            array.append(dic)
        return array
    elif typ == 'oldlist':
        mydict = {}
        try:
            if not string == '':
                try:
                    elements = string.split(',')
                except:
                    return {}
                for el in elements:
                    dat = el.split('_')
                    mydict[dat[0]] = dat[1]
        except:
            return mydict
        return mydict
    elif typ == 'olddeltadict':  # remove when all inputs are converted
        # Delta Dictionary looks like
        # A2_2015_0.00_0.00_201510_-0.13,A2_2016_0.00_0.00_201610_-0.06,A2_2017_0.00_0.00_201707_-0.03
        pierdic = {}
        liste = string.strip().split(',')
        # Extract piers:
        pierlist = []
        for el in liste:
            pier = el.split('_')[0].strip()
            pierlist.append(pier)
        pierlist = list(set(pierlist))
        for pier in pierlist:
            yeardic = {}
            for el in liste:
                valdic = {}
                vals = el.split('_')
                if len(vals) == 6 and vals[0] == pier:
                    if not vals[2] == '0.00':  # not determined
                        valdic['deltaD'] = vals[2]
                    if not vals[3] == '0.00':  # not determined
                        valdic['deltaI'] = vals[3]
                    if vals[4][:4] == vals[1]:  # only add year
                        valdic['deltaF'] = vals[5]
                    yeardic[vals[1]] = valdic
                    # Eventually add f year
                    if yeardic.get(vals[4][:4], '') == '':
                        valdic = {}
                        valdic['deltaF'] = vals[5]
                        yeardic[vals[4][:4]] = valdic
            pierdic[pier] = yeardic
        return pierdic
    else:
        array = []
        liste = string.split(';')
        for el in liste:
            line = el.split(',')
            array.append(line)
        return array


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
        elif isinstance(time, str):  # test for str only in Python 3 should be basestring for 2.x
            try:
                timeobj = datetime.strptime(time, "%Y-%m-%d")
            except:
                try:
                    timeobj = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")
                except:
                    try:
                        timeobj = datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f")
                    except:
                        try:
                            timeobj = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f")
                        except:
                            try:
                                timeobj = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
                            except:
                                try:
                                    # Not happy with that but necessary to deal
                                    # with old 1000000 micro second bug
                                    timearray = time.split('.')
                                    if timearray[1] == '1000000':
                                        timeobj = datetime.strptime(timearray[0], "%Y-%m-%d %H:%M:%S")+timedelta(seconds=1)
                                    else:
                                        # This would be wrong but leads always to a TypeError
                                        timeobj = datetime.strptime(timearray[0], "%Y-%m-%d %H:%M:%S")
                                except:
                                    try:
                                        timeobj = num2date(float(time)).replace(tzinfo=None)
                                    except:
                                        raise TypeError
        elif isinstance(time, np.datetime64):
            unix_epoch = np.datetime64(0, 's')
            one_second = np.timedelta64(1, 's')
            seconds_since_epoch = (time - unix_epoch) / one_second
            timeobj = datetime.utcfromtimestamp(float(seconds_since_epoch))
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
                   "%Y-%m-%dT%H:%M:%SZ",
                   "%Y-%m-%dT%H:%M:%S.%fZ"
                   ]

    basestring = (str, bytes)
    j = 0
    timeobj = time

    if isinstance(time, float) or isinstance(time, int):
        try:
            timeobj = num2date(time).replace(tzinfo=None)
        except:
            raise TypeError
    elif isinstance(time, basestring):  # test for str only in Python 3 should be basestring for 2.x
        for i, tf in enumerate(timeformats):
            try:
                timeobj = datetime.strptime(time, tf)
                break
            except:
                pass
    elif isinstance(time, str):  # test for str only in Python 3 should be basestring for 2.x
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
                        timeobj = datetime.strptime(timearray[0], "%Y-%m-%d %H:%M:%S")+timedelta(seconds=1)
                    else:
                        # This would be wrong but leads always to a TypeError
                        timeobj = datetime.strptime(timearray[0], "%Y-%m-%d %H:%M:%S")
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
    # tested elswhere:
    # - func_to_file and func_from_file in stream together with functiontools

    errors = {}
    testdate = "1971-11-22T11:20:00"
    teststring = "xxx"
    testnumber = 123.123
    testarray1 = np.array([1.23, 23.45, np.nan, 2.45])
    testarray2 = np.array([datetime(2024, 11, 22, 5), datetime(2024, 11, 22, 6), "", datetime(2024, 11, 22, 9)])
    testarray3 = np.array([datetime(2024, 11, 22, 5), datetime(2024, 11, 22, 6), datetime(2024, 11, 22, 9), datetime(2024, 11, 22, 11)])
    v = np.array([1, 2, 3, 4, 5, np.nan, 7, 8, 9, 10, 11, 12, 13, 14])
    indlist = [0,2,3,2000,2005,2006,2007,2008,2034,2037,2040,2041,2042,2050]
    d1 = {}
    d2 = {}
    try:
        var1 = is_number(teststring)
        var2 = is_number(testnumber)
    except Exception as excep:
        errors['is_number'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR testing number.")

    try:
        var1 = testtime(testdate)
        print(var1)
        var2 = ceil_dt(var1, 3600)
        print("Rounded to hour by ceil_dt", var2)
    except Exception as excep:
        errors['testdate'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR testdate.")

    try:
        lon, lat = convert_geo_coordinate(-34833.41399,310086.6051,'epsg:31256','epsg:4326')
        print ("Longitude: {}, Latitude: {}".format(lon,lat))
    except Exception as excep:
        errors['convert_geo_coordinate'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR convert_geo_coordinate.")

    try:
        chunks = get_chunks(86400, wl=3600)
    except Exception as excep:
        errors['chunks'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR chunks.")

    try:
        datelist = extract_date_from_string("ccc_2022-11-22.txt")
    except Exception as excep:
        errors['extract_date_from_string'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR extract_date_from_string.")

    try:
        for fill in ['mean', 'interpolate', 'value']:
            mv = missingvalue(v, window_len=10, fill=fill, fillvalue=99)
            print("filling option {}: {}".format(fill, mv))
    except Exception as excep:
        errors['missingvalue'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with missingvalue.")

    try:
        a, b = find_nearest(v, 10.3)
        print(a, b)
    except Exception as excep:
        errors['find_nearest'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with find_nearest.")

    try:
        i = find_nth("Hello_World_I_am_here","_", 3)
        print(i)
    except Exception as excep:
        errors['find_nth'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with find_nth.")
    try:
        group = group_indices(indlist)
        # eventually implement the following with unittest
        if not group == [[0, 0], [3, 3], [8, 8], [9, 9], [13, 13], [1, 2], [4, 7], [10, 12]]:
            print (" group_indices: verification failure")
    except Exception as excep:
        errors['group_indices'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR group_indices.")
    try:
        a = test_timestring(testdate)
    except Exception as excep:
        errors['test_timestring'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with test_timestring.")
    try:
        ar1 = maskNAN(testarray1)
        ar2 = maskNAN(testarray2)
    except Exception as excep:
        errors['mask_nan'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR maskNAN.")
    try:
        dround = round_second(datetime.now(timezone.utc).replace(tzinfo=None))
    except Exception as excep:
        errors['round_second'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR round_second.")
    try:
        d1 = string2dict('A2_(2017_(deltaD_0.00;deltaI_0.201;deltaF_1.12);2018_(deltaF_1.11))')
        d2 = string2dict('st_736677.0,time_timedelta(seconds=-2.3),et_736846.0', typ='listofdict')
    except Exception as excep:
        errors['string2dict'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR string2dict.")
    try:
        t1 = dict2string(d1)
        t2 = dict2string(d2, typ='listofdict')
    except Exception as excep:
        errors['dict2string'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR dict2string.")
    try:
        da = {0:"a",1:"b",2:"c"}
        db = {0:"a",1:"b",2:"d"}
        res = dictdiff(da, db, show_value_diff=True)
    except Exception as excep:
        errors['dictdiff'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR dictdiff.")
    try:
        result = dicgetlast(d1, pier='A2', element='deltaD,deltaI,deltaF')
        print (result)
    except Exception as excep:
        errors['dictgetlast'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR dictgetlast.")

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
            print("{} : errormessage = {}".format(item, errors.get(item)))
