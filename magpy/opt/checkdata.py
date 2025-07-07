# -*- coding: utf-8 -*-

"""
checkdata contains methods to support geomagnetic data checking

the following methods are contained:

|class | method | since version | until version | runtime test | result verifi. | manual | *used by       |
|----- | ------ | ------------- | ------------- | ------------ | -------------- | ------ | -------------- |
|**opt.checkdata** |  |          |               |              |              |        |   |
|    | _delta_F_test    | 2.0.0 |               | yes          |              |        |   |
|    | check_minute_directory |  2.0.0 |        | yes          |              |        |   |
|    | convert_geo_coordinate | 2.0.0 |         | yes          |              |        |   |
|    | read_month       | 2.0.0 |               | yes          |              |        |   |
|    | consistency_test | 2.0.0 |               | yes          |              |        |   |
|    | content_test     | 2.0.0 |               | yes          |              |        |   |
|    | baseline_check   | 2.0.0 |               | yes          |              |        |   |
|    | header_test      | 2.0.0 |               | yes          |              |        |   |
|    | k_value_test     | 2.0.0 |               | yes          |              |        |   |
"""


import sys
sys.path.insert(1, '//')  # should be magpy2

import numpy as np
import os
from datetime import datetime, timedelta, timezone
from matplotlib.dates import date2num
import glob
from magpy.stream import read, DataStream, magpyversion, merge_streams, subtract_streams
from magpy.core import methods
from magpy.lib.magpy_formats import IAFBINMETA, IAGAMETA, IMAGCDFMETA


def _delta_F_test(fdata, debug=False):
    """
    DESCRIPTION
         Testing delta F values
         One could also run the outlier method on delta F
    CALLED BY
         consistency_test
    RETURNS
         dfmean     : mean of delta F -> offset from zero might indicate baseline correction issue
         dfmedian     : median of delta F -> difference of median and mean as indicator for outliers
         dfstddev   : stddev of delta F -> large variance indicates spikes or uncompensated baseline components
         fsamprate  : samplingrate of F
    """
    result = {}
    # Get the f and df columns  and test for data existence
    ftest = fdata.copy()
    fcol = fdata._get_column('f')
    dfcol = fdata._get_column('df')
    if len(fcol) == 0 and len(dfcol) == 0:
        if debug:
            print (" No F or dF values found")
        return {'dF mean' : 0., 'dF median' : 0., 'dF stddev' : 0., 'F rate': 0., 'dF test' : "no data"}
    scal=''
    msg = "data found: "
    if len(dfcol) > 0:
        if debug:
            print (" dF values found")
        scal = 'df'
        msg += scal
    if len(fcol) > 0:
        if debug:
            print (" F values found")
        scal = 'f'
        if msg.endswith('f'):
            msg += ", "
        msg += scal
    result['dF test'] = msg
    #print (ftest)
    ftest = ftest._drop_nans(scal)
    #print (ftest)
    result['F rate'] = ftest.samplingrate()
    if scal=='f':
        if debug:
            print (" F provided, calculating dF")
        ftest = ftest.delta_f()

    #TODO proper treatment of -S values in delta_f in MagPy
    # or ignore: G is quality value for variometer data and
    # should be provided for existing variometer values.
    # If F(S) should be provided, an independent measurement with
    # with eventually different sampling rate or data value at non-existing
    # variometer data, then please provide it as S. G can be easily calculated
    #quick workaround -> exclude large negative  values
    ftest = ftest.extract('df',-15000,'>')

    fmean, fstd = ftest.mean('df',std=True, percentage=1)
    fmedian, fstd = ftest.mean('df',meanfunction='median',std=True, percentage=1)
    result['dF mean'] = fmean
    result['dF median'] = fmedian
    result['dF stddev'] = fstd
    if debug:
        print ("F result", result)
    return result


def check_minute_directory(config, results):
    """
    DESCRIPTION
        Load directory and check file contents, extensions, amount, names.
        Fill results dictionary with directory information.
        This method support data directories containing IAF, IAGA-2002 and IMAGCDF one-minute archives
    TESTING DETAILS:
        1) Checks if an appropriate amount of files is contained in the selected directory
            (i.e. 12 BIN, 365/366 IAGA2002 or at least on IMAGCDF file)
            grade 1 if all, grade 2 if individual missing files, grade 3 if all are missing
        2) Checks if README, BLV and YEARMEAN files are existing
            grade 1 if all, grade 2 if individual missing files
        3) Checks auxiliary data is present
        4) extracts year (datafile names) and stationid (YEARMEAN extension)
    :param config:
    :param results:
    :return:
    """
    if not results:
        results = { "report" : [],
                    "warnings" : [],
                    "errors" : [],
                    "grades" : { "step1" : 0
                                 }
                    }
    minutepath = config.get('mindatapath')
    months = config.get('months')
    stationid = "XXX"
    year = None
    grades = results.get('grades',{})
    res_min_dir = {}
    if grades.get("step1",0) <= 1:
        grades["step1"] = 1
    res_min_dir["report"] = ["\n### 1.1 One-minute directory analysis\n"]
    res_min_dir["year"] = year
    res_min_dir["stationid"] = stationid
    res_min_dir["mindatacheck"] = ""
    res_min_dir["blvdatacheck"] = []
    res_min_dir["yearmeancheck"] = []
    res_min_dir["readmecheck"] = []
    res_min_dir["dkacheck"] = []

    if not minutepath:
        res_min_dir["report"].append(" no minute data path selected - skipping")
        results['minute-data-directory'] = res_min_dir
        return results
    elif not os.path.isdir(minutepath):
        # add some report to results
        res_min_dir["report"].append(" failed - could not access one-minute path: {}".format(minutepath))
        results["error"].append("Check minute directory: given data path not accessible")
        if grades.get("step1") <= 3:
            grades["step1"] = 3
        results['minute-data-directory'] = res_min_dir
        return results
    else:
        res_min_dir["report"].append(" checking directory structure and contents")
        # get a list of files in the selected directory
        allfilelist = glob.glob(os.path.join(minutepath, "*"))
        res_min_dir['mindatafiles'] = allfilelist
        minutesummary = {}
        for fn in allfilelist:
            fl = os.path.splitext(os.path.basename(fn))
            fname = fl[0]
            extension = fl[1]
            ext = extension.replace(".", "").lower()
            if not minutesummary.get(ext):
                counter = len(glob.glob1(minutepath, "*{}".format(extension)))
                minutesummary[ext] = counter
            if ext == 'blv':
                fname1 = fname.lower()
                # assume that 2050 onwards IAF is not used any more and data before 1950 is not checked
                year = int(fname1[-2:]) + 2000 if int(fname1[-2:]) < 50 else int(fname1[-2:]) + 1900
                res_min_dir["blvdatacheck"].append(fn)
            elif ext == 'dka':
                res_min_dir["dkacheck"].append(fn)
            elif ext == 'bin':
                for mon in config.get("months"):
                    month = datetime(1900, mon, 1).strftime('%b')
                    if month.lower() in fname.lower():
                        fname1 = fname.lower().replace(month.lower(),'')
                        # assume that 2050 onwards IAF is not used any more and data before 1950 is not checked
                        if not year:
                            year = int(fname1[-2:])+2000 if int(fname1[-2:])<50 else int(fname1[-2:])+1900
                        res_min_dir["mindatacheck"] = extension
            elif ext == 'min':
                dmon = methods.extract_date_from_string(fname)[0]
                if not year:
                    year = dmon.year
                if dmon:
                    for mon in config.get("months"):
                        if int(dmon.month) == mon and not res_min_dir.get("mindatacheck"): # only if bin extension is not found
                            res_min_dir["mindatacheck"] = extension
            if fname.lower() == 'readme':
                res_min_dir["report"].append(" - found README file for IMO: {}".format(extension.replace(".",'')))
                res_min_dir["readmecheck"].append(fn)
            if fname.lower() == 'yearmean':
                res_min_dir["yearmeancheck"].append(fn)
                stationid = extension.replace(".","").upper()
                res_min_dir["report"].append(" - found YEARMEAN file for IMO: {}".format(extension.replace(".",'')))
        if minutesummary.get('bin',0) == 12 or minutesummary.get('min',0) >= 365 or minutesummary.get('cdf',0) >= 1:
            res_min_dir["report"].append(" - found correct number of data files")
            if minutesummary.get('bin',0) == 12:
                res_min_dir["format"] = "IAF"
            elif minutesummary.get('min',0) >= 365:
                res_min_dir["format"] = "IAGA-2002"
            elif minutesummary.get('cdf',0) >= 1:
                res_min_dir["format"] = "IMAGCDF"
        elif not minutesummary.get('bin') and not minutesummary.get('min') and not minutesummary.get('cdf'):
            if grades.get("step1") <= 3:
                grades["step1"] = 3
            results["errors"].append("Check minute directory: no data files found")
        else:
            if grades.get("step1") <= 2:
                grades["step1"] = 2
            results["warnings"].append("Check minute directory: incorrect amount of data files")
        if minutesummary.get('blv',0) >= 1:
            res_min_dir["report"].append(" - found baseline data file")
        else:
            if grades.get("step1") <= 3:
                grades["step1"] = 3
            results["errors"].append("Check minute directory: no baseline file")
        if minutesummary.get('dka',0) >= 1:
            res_min_dir["report"].append(" - found k-value DKA file")
        if minutesummary.get('png',0) >= 1:
            res_min_dir["report"].append(" - auxiliary files are present")
        if not len(res_min_dir.get("yearmeancheck")) > 0:
            if grades.get("step1") <= 3:
                grades["step1"] = 3
            results["errors"].append("Check minute directory: no yearmean file")
        if not len(res_min_dir.get("readmecheck")) > 0:
            if grades.get("step1") <= 3:
                grades["step1"] = 3
            results["errors"].append("Check minute directory: no readme file")

    if not year:
        print ("Warning - year could not be obtained")
        year = 1777
    res_min_dir["year"] = year
    res_min_dir["stationid"] = stationid
    results['grades'] = grades
    results['minute-data-directory'] = res_min_dir

    return results


def check_second_directory(config, results):
    """
    DESCRIPTION
         Load directory and check file contents, extensions, amount, names.
        Fill results dictionary with directory information.
        This method support data directories containing IAGA-2002 and IMAGCDF one-second archives
    TESTING DETAILS:
        1) Checks if an appropriate amount of files is contained in the selected directory
            (i.e. 12/365/366 IMAGCDF, 365/366 IMAGCDF IAGA2002
            grade 1 if all, grade 2 if individual missing files, grade 3 if all are missing
        2) extracts year (datafile names)

    :param config:
    :param results:
    :return:
    """
    if not results:
        results = { "report" : [],
                    "warnings" : [],
                    "errors" : [],
                    "grades" : { "step1" : 0
                                 }
                    }
    secondpath = config.get('secdatapath')
    months = config.get('months')
    stationid = "XXX"
    year = 1777
    grades = results.get('grades',{})
    if grades.get("step1",0) <= 1:
        grades["step1"] = 1
    res_sec_dir = {}
    res_sec_dir["year"] = year
    res_sec_dir["report"] = ["\n###  1.2 One-second directory analysis\n"]
    res_sec_dir["secdatacheck"] = ""

    if not secondpath:
        res_sec_dir["report"].append(" no second data path selected - skipping")
        results['second-data-directory'] = res_sec_dir
        return results
    elif not os.path.isdir(secondpath):
        # add some report to results
        res_sec_dir["report"].append(" failed - could not access one-second path: {}".format(secondpath))
        results["errors"].append("Check second directory: given data path not accessible")
        if grades.get("step1") <= 3:
            grades["step1"] = 3
        results['second-data-directory'] = res_sec_dir
        return results
    else:
        res_sec_dir["report"].append(" checking directory structure and contents")
        # get a list of files in the selected directory
        allfilelist = glob.glob(os.path.join(secondpath, "*"))
        res_sec_dir['secdatafiles'] = allfilelist
        secondsummary = {}
        for fn in allfilelist:
            fl = os.path.splitext(os.path.basename(fn))
            fname = fl[0]
            extension = fl[1]
            ext = extension.replace(".", "").lower()
            if not secondsummary.get(ext):
                counter = len(glob.glob1(secondpath, "*{}".format(extension)))
                secondsummary[ext] = counter
            if ext in ['cdf', 'sec']:
                dmon = methods.extract_date_from_string(fname)[0]
                year = dmon.year
                if dmon:
                    for mon in config.get("months"):
                        if int(dmon.month) == mon:
                            res_sec_dir["secdatacheck"] = extension

        if secondsummary.get('cdf',0) == 12 or secondsummary.get('cdf',0) >= 365:
            res_sec_dir["report"].append(" - found correct number of ImagCDF data files")
            res_sec_dir["format"] = "IMAGCDF"
        elif secondsummary.get('sec',0) >= 365:
            res_sec_dir["format"] = "IAGA-2002"
            res_sec_dir["report"].append(" - found correct number of IAGA-2002 data files")
        elif not secondsummary.get('cdf') and not secondsummary.get('sec'):
            if grades.get("step1") <= 3:
                grades["step1"] = 3
            results["error"].append("Check second directory: no data files found")
        else:
            if grades.get("step1") <= 2:
                grades["step1"] = 2
            results["warnings"].append("Check second directory: incorrect amount of data files")

    res_sec_dir["year"] = year
    results['grades'] = grades
    results['second-data-directory'] = res_sec_dir

    return results


def read_month(config, results, month=1, debug=False):
    """
    DESCRIPTION
         Load data from a single month and check readability and meta information.
         Add paths of files to check to config.

    :param config:
    :param results:
    :return:
    """
    if not results:
        results = { "report" : [],
                    "warnings" : [],
                    "errors" : [],
                    "temporaryminutedata" : DataStream(),
                    "temporaryseconddata" : DataStream(),
                    "grades" : { "step2" : 0
                                 }
                    }
    grades = results.get('grades',{})
    if grades.get("step2",0) <= 1:
        grades["step2"] = 1
    res_read_test = {}
    for index, resolution in enumerate(["minute", "second"]):
        if debug:
            print ("Running for resolution", resolution)
        res_dir = results.get('{}-data-directory'.format(resolution))
        datapath = config.get('{}datapath'.format(resolution[:3]))
        logdict = {}
        logdict["report"] = ["\n#### 2.{}.{} One-{} read test\n".format(month, index+1, resolution)]
        if res_dir:
            if debug:
                print(" - {} available: continuing".format(resolution))
            year = res_dir.get('year')
            ext = res_dir.get("{}datacheck".format(resolution[:3]))
            if grades.get("step2") <= 1:
                grades["step2"] = 1
            # get time range to check
            startdate = datetime(year,month,1)
            enddate = startdate+timedelta(days=32)
            enddate = datetime(enddate.year, enddate.month, 1)
            if ext:  # found data
                if debug:
                    print (" - loading from directory:", os.path.join(datapath, "{}{}".format('*',ext)))
                logdict['Data path'] = os.path.join(datapath, "*"+ext)
                data = read(os.path.join(datapath, "{}{}".format('*',ext)), starttime=startdate, endtime=enddate)
                if debug:
                    print (" - got {} datapoints".format(len(data)))
                    print (os.path.join(datapath, "{}{}".format('*',ext)), startdate ,enddate)
                if not len(data) > 0:
                    # eventually start and endtime could not be identified in filename like for IAF, so read all and trim
                    data = read(os.path.join(datapath, "{}{}".format('*',ext)))
                    data = data.trim(starttime=startdate, endtime=enddate)
                # get month and year
                if not len(data) > 0:
                    logdict["report"].append(" - data could not be read -aborting")
                    results["errors"].append("Read test: {} data could not be read for month {}".format(resolution, month))
                    if grades.get("step2") <= 3:
                        grades["step2"] = 3
                else:
                    results['temporary{}data'.format(resolution)] = data
                    logdict['data'] = data
                    logdict["report"].append(" - data for month {} successfully loaded ".format(month))
                    cntbefore = len(data)
                    data = data.get_gaps()
                    cntafter = len(data)
                    st, et = data.timerange()
                    effectivedays = int(date2num(et) - date2num(st)) + 1
                    multi = 1
                    expectedsr = 60
                    if resolution == 'second':
                        multi = 60
                        expectedsr = 1
                    expectedcount = multi*1440*effectivedays
                    logdict['Data limits'] = [st, et]
                    logdict['N'] = cntbefore
                    logdict['Filled gaps'] = cntafter - cntbefore
                    logdict['Data header'] = data.header
                    logdict['Difference to expected amount'] = expectedcount - cntafter
                    sr = data.samplingrate()
                    logdict['Samplingrate'] = '{} sec'.format(sr)
                    if not (expectedcount - cntafter) == 0:
                        logdict["report"].append(' - amount of data points {} does not correspond to the expected amount {}'.format(
                                cntafter, expectedcount))
                        if grades.get("step2") <= 2:
                            grades["step2"] = 2
                    if (effectivedays * 24 * 3600) < cntafter:
                        logdict["report"].append(' - more data than expected: check data files for duplicates and correct coverage')
                        if grades.get("step2") <= 2:
                            grades["step2"] = 2
                    if not int(sr) == expectedsr:
                        logdict["report"].append(' - found sampling rate of {} sec, expected is {} sec'.format(sr, expectedsr))
                        if grades.get("step2") <= 3:
                            grades["step2"] = 3

                    dataformat = data.header.get('DataFormat')
                    logdict['Data format'] = dataformat
                    if debug:
                        print (" - logdict looks like", logdict)
                    # checking header
                    headfailure = False
                    referencemeta = {}
                    if dataformat == 'IMAGCDF': # and sr==1:
                        referencemeta = IMAGCDFMETA
                    elif dataformat == 'IAGA':
                        referencemeta = IAGAMETA
                    elif dataformat == 'IAF':
                        referencemeta = IAFBINMETA
                    for head in referencemeta:
                        value = data.header.get(head, '')
                        if value == '':
                            results["warnings"].append("Read test for {} data, month {}: no meta information for {}".format(resolution, month, head))
                            headfailure = True
                            if grades.get("step2") <= 2:
                                grades["step2"] = 2
                    if not headfailure:
                        logdict["report"].append(" - all header elements present - did not check contents however")

                    # TODO
                    #logdict['Leap second update'] = data.header.get('DataLeapSecondUpdated')
                    #if not str(latestleapsecond) == str(data.header.get('DataLeapSecondUpdated')):
                    #    warningdict['Leap second'] = 'Leap second table seems to be outdated - please check'

        res_read_test[resolution] = logdict

        if debug:
            print (" ... resolution {} run done".format(resolution))

    results[month] = res_read_test

    return results


def consistency_test(config, results, month=1, debug=False):
    """
    DESCRIPTION
         Testing internal consistency of each data set. This includes the completeness of vector data,
         dalta F values and its offset, variation, auxiliary data like temperature, other resolution data
         like hours, k values.
         Consistency testing is done for each month directly after read month making use of temporary files.

    :param config:
    :param results:
    :return:
    """
    if not results:
        results = { "report" : [],
                    "warnings" : [],
                    "errors" : [],
                    "temporaryminutedata" : DataStream(),
                    "temporaryseconddata" : DataStream(),
                    "grades" : { "step2" : 0
                                 }
                    }
    grades = results.get('grades',{})
    monthdict = {}
    if grades.get("step2",0) <= 1:
        grades["step2"] = 1
    #res_cons_test = {}
    for index, resolution in enumerate(["minute", "second"]):
        if debug:
            print ("Running consistency test for resolution", resolution)
        monthdict = results.get(month)
        logdict = monthdict.get(resolution)
        data = results.get('temporary{}data'.format(resolution))
        if logdict and data:
            logdict["report"].append("\n#### 2.{}.{} One-{} consistency test\n".format(month, index+3, resolution))
            # Testing F/G
            # -----------------------------
            # read scalar data if applicable
            fc = data.header.get('FileContents',[])
            logdict["amount of scalar data"] = len(data)
            if fc and len(fc) > 0:
                logdict["report"].append(" - data set contains contents of variable lengths: {}".format(data.header.get('FileContents')))
                vlen = [el[0] for el in fc if el[1].find("Vector") >= 0]
                slen = [el[0] for el in fc if el[1].find("Scalar") >= 0]
                logdict["amount of scalar data"] = slen[0]
                if len(vlen) > 0 and len(slen) > 0: # and not vlen[0] == slen[0]:  might have the same lengths
                    logdict["report"].append(" - found different amounts of scalar data N={} and variometer data N={}".format(slen[0], vlen[0]))
                    logdict["report"].append("   filtering both data sets and merging at equal time steps before delta F analysis")
                    if debug:
                        print (" extracting and filtering scalar data before delta F analysis")
                    scalardata = read(logdict.get("Data path"), starttime=logdict.get("Data limits")[0], endtime=logdict.get("Data limits")[1], select="scalar")
                    scalardata = scalardata.filter()
                    vectordata = data.filter()
                    data = merge_streams(vectordata,scalardata)
                    logdict["filtered"] = data.copy()
            #if logdict.get("Data format").find("CDF") > -1:
            # read specific f data in case of IMAGCDF files
            # get data path and call command to read scalar data
            fdata = data.copy()
            fresult = _delta_F_test(fdata, debug=debug)
            if fresult.get('dF test').startswith('no'):
                logdict["amount of scalar data"] = 0
            else:
                logdict["report"].append(" - delta F analysis: sampling rate = {:.0f} sec".format(fresult.get('F rate',0)))
                logdict["report"].append(" - delta F analysis: obtained average dF={:.3f}, median dF={:.3f} and a standard deviation={:.3f}".format(fresult.get('dF mean'),fresult.get('dF median'),fresult.get('dF stddev')))
                logdict["average deltaF"] = fresult.get('dF mean')
                logdict["median deltaF"] = fresult.get('dF median')
                logdict["standard deviation deltaF"] = fresult.get('dF stddev')
                if np.isnan(fresult.get('dF mean')):
                    results["warnings"].append("Consistency test: Month {}, {} resolution: invalid F data".format(month,resolution))
                    if grades.get("step2", 0) <= 2:
                        grades["step2"] = 2
                elif np.abs(fresult.get('dF mean')) >= 1.0:
                    results["warnings"].append("Consistency test: Month {}, {} resolution: average delta F differs strongly from zero".format(month,resolution))
                    if grades.get("step2", 0) <= 2:
                        grades["step2"] = 2
                elif np.abs(fresult.get('dF mean')) < 0.01 and np.abs(fresult.get('dF stddev')) < 0.01:
                    results["warnings"].append("Consistency test: Month {}, {} resolution: is F really independent from vector data?".format(month,resolution))
                    if grades.get("step2", 0) <= 2:
                        grades["step2"] = 2
                #print ("Diff between mean and median", np.abs(fresult.get('dF mean') - fresult.get('dF median')))
                # Better check whether they are consitent within their uncertainty range
                if not np.isnan(fresult.get('dF mean')) and np.abs(fresult.get('dF mean') - fresult.get('dF median')) > 0.1:
                    results["warnings"].append("Consistency test: Month {}, {} resolution: Median and Mean are significantly different. Check outliers".format(month,resolution))
                    if grades.get("step2", 0) <= 2:
                        grades["step2"] = 2
            # Testing temperature
            # -----------------------------
            if fc and len(fc) > 0:
                tempdata = read(logdict.get("Data path"), starttime=logdict.get("Data limits")[0],
                                  endtime=logdict.get("Data limits")[1], select="temperature")
            else:
                tempdata = data.copy()
            t1col = tempdata._get_column('t1')
            t2col = tempdata._get_column('t2')
            if len(t1col) > 0:
                tdata = tempdata._drop_nans('t1')
                t1mean, t1std = tdata.mean('t1', std=True)
                logdict["report"].append(" - temperature analysis: average T1 {:.2f} +/- {:.2f} deg".format(t1mean, t1std))
            if len(t2col) > 0:
                tdata = tempdata._drop_nans('t2')
                t2mean, t2std = tdata.mean('t2', std=True)
                logdict["report"].append(" - temperature analysis: average T2 {:.2f} +/- {:.2f} deg".format(t2mean, t2std))
            # Testing filtered contents
            # -----------------------------
            if logdict.get("Data format").find("IAF") > -1:
                logdict["report"].append(
                    " - testing hourly mean data in IAF")
                #print ("CURRENT results", results)
                hour_filter = data.filter(filter_type='flat',filter_width=timedelta(hours=1),resampleoffset=timedelta(minutes=30))
                st = logdict.get("Data limits")[0]
                et = logdict.get("Data limits")[1]
                hour_data = read(logdict.get("Data path"), starttime=st, endtime=et, resolution="hour")
                if len(hour_data) > 0:
                    effectivedays = int(date2num(et) - date2num(st)) + 1
                    expectedlength = 24*effectivedays
                    if not len(hour_data) == expectedlength:
                        results["warnings"].append(
                            "Consistency test: Month {}, {} resolution: IAF hour data N={} differs from expected amount N={}".format(
                                month,resolution,len(hour_data),expectedlength))
                        if grades.get("step2", 0) <= 2:
                            grades["step2"] = 2
                    elif not len(hour_filter) == len(hour_data):
                        logdict["report"].append(
                            " - hourly data from IAF and filtered minute data differ in length, not affecting grade but please check")
                    else:
                        logdict["report"].append(
                            " - extracted hourly data from IAF: found the expected amount of data")
                    diffs = subtract_streams(hour_filter, hour_data)
                    hourwarn = False
                    if np.isnan(diffs.mean('x')) or np.abs(diffs.mean('x')) > 0.05:
                        results["warnings"].append(
                            "Consistency test: Month {}, {} resolution: IAF hour data differs from filtered minute data. Difference in X={}".format(
                                month,resolution,diffs.mean('x')))
                        if grades.get("step2", 0) <= 2:
                            grades["step2"] = 2
                        hourwarn = True
                    if np.isnan(diffs.mean('y')) or np.abs(diffs.mean('y')) > 0.05:
                        results["warnings"].append(
                            "Consistency test: Month {}, {} resolution: IAF hour data differs from filtered minute data. Difference in Y={}".format(
                                month,resolution,diffs.mean('y')))
                        if grades.get("step2", 0) <= 2:
                            grades["step2"] = 2
                        hourwarn = True
                    if np.isnan(diffs.mean('z')) or np.abs(diffs.mean('z')) > 0.05:
                        results["warnings"].append(
                            "Consistency test: Month {}, {} resolution: IAF hour data differs from filtered minute data. Difference in Z={}".format(
                                month,resolution,diffs.mean('z')))
                        if grades.get("step2", 0) <= 2:
                            grades["step2"] = 2
                        hourwarn = True
                    #if np.isnan(diffs.mean('df')) or np.abs(diffs.mean('df')) > 0.05:
                    #    # TODO if no df in hourly data than diff will np.nan - add that in the README
                    #    results["warnings"].append(
                    #        "Consistency test: Month {}, {} resolution: IAF hour data differs from filtered minute data. Difference in dF={}".format(
                    #            month,resolution,diffs.mean('df')))
                    #    hourwarn = True
                    if not hourwarn:
                        logdict["report"].append(
                            " - compared hourly data from IAF with filtered one-minute data: fine")
                else:
                    logdict["report"].append(
                    " - could not extract hourly data from IAF")
                    if grades.get("step2", 0) <= 3:
                        grades["step2"] = 3
                    results["errors"].append(
                        "Consistency test: Month {}, {} resolution: Could not extract IAF hourly data".format(
                            month, resolution))

        monthdict[resolution] = logdict

    results[month] = monthdict

    return results


def content_test(config, results, month=1, debug=False):
    """
    DESCRIPTION
         Content test compares one-second and one-minute data. This step requires the availability of both data sets
         and is skipped if such data is not available.
         Content testing is done for each month directly and requires consistency tests before.

    :param config:
    :param results:
    :return:
    """
    if not results:
        results = { "report" : [],
                    "warnings" : [],
                    "errors" : [],
                    "temporaryminutedata" : DataStream(),
                    "temporaryseconddata" : DataStream(),
                    "grades" : { "step2" : 0
                                 }
                    }
    grades = results.get('grades',{})
    if grades.get("step2",0) <= 1:
        grades["step2"] = 1
    # content test starts with one second
    resolution = "second"
    monthdict = results.get(month)
    logdict = monthdict.get(resolution)
    # check if filtered data is available already
    filtdata = logdict.get('filtered', DataStream())
    if not len(filtdata) > 0:
        data = results.get('temporary{}data'.format(resolution))
        if data and len(data) > 0:
            filtdata = data.filter()
    # get one-minute data:
    mindata = results.get('temporaryminutedata')
    if len(mindata) > 0 and len(filtdata) > 0:
        if debug:
            print ("Content test: Lengths of filtered data sets")
            print (len(mindata))
            print (len(filtdata)) # filtered data might be shorter than mindata because of filterwindow
        if len(mindata) == len(filtdata):
            # remove first and last time step
            diff = subtract_streams(filtdata, mindata, keys=['x', 'y', 'z'])
            logdict['diffdata'] = diff
            # drop the first time step - quick and dirty - remove if filtering has been checked
            diff = diff.trim(starttime=diff.ndarray[0][1], endtime=diff.ndarray[0][-2])
            xd, xdst = diff.mean('x', std=True)
            yd, ydst = diff.mean('y', std=True)
            zd, zdst = diff.mean('z', std=True)
            try:
                xa = diff.amplitude('x')
                ya = diff.amplitude('y')
                za = diff.amplitude('z')
            except:
                print("Problem determining amplitudes...")
                xa = 0.00
                ya = 0.00
                za = 0.00
            if debug:
                print("  -> amplitudes determined")
            logdict['filtered vs minutedata: mean difference - x component'] = "{:.3} nT".format(xd)
            logdict['filtered vs minutedata: mean difference - y component'] = "{:.3} nT".format(yd)
            logdict['filtered vs minutedata: mean difference - z component'] = "{:.3} nT".format(zd)
            logdict['filtered vs minutedata: stddev of difference - x component'] = "{:.3} nT".format(xdst)
            logdict['filtered vs minutedata: stddev of difference - y component'] = "{:.3} nT".format(ydst)
            logdict['filtered vs minutedata: stddev of difference - z component'] = "{:.3} nT".format(zdst)
            logdict['filtered vs minutedata: amplitude of difference - x component'] = "{:.3} nT".format(xa)
            logdict['filtered vs minutedata: amplitude of difference - y component'] = "{:.3} nT".format(ya)
            logdict['filtered vs minutedata: amplitude of difference - z component'] = "{:.3} nT".format(za)
            if debug:
                print("  -> dictionary written")
            if max(xd, yd, zd) > 0.3:
                results["warnings"].append('Content check for month {}: one-minute and one-second data differ by more than 0.3 nT in monthly average'.format(month))
                if grades.get("step2", 0) <= 2:
                    grades["step2"] = 2
            if max(xa, ya, za) < 0.12:
                logdict[
                    'report'].append(' - content check: excellent agreement between definitive one-minute and one-second data products')
            elif max(xa, ya, za) <= 0.3:
                logdict[
                    'report'].append(' - content check: good agreement between definitive one-minute and one-second data products')
            elif max(xa, ya, za) > 0.3 and max(xa, ya, za) <= 5:
                logdict[
                    'report'].append(' - content check: small differences in peak amplitudes between definitive one-minute and one-second data products observed')
            elif max(xa, ya, za) > 5:
                results["warnings"].append('Content check for month {}: Large amplitude differences between definitive one-minute and one-second data products'.format(month))
                if grades.get("step2", 0) <= 2:
                    grades["step2"] = 2
            if np.isnan(sum([xd, yd, zd, xa, ya, za])):
                logdict[
                    'report'].append(' - content check: not conclusive as NAN values are found')
            if debug:
                print("  -> one-minute comparison finished")
        else:
            results["warnings"].append('Comparison with definitive one-minute: filtered and original data sets differ in length')
            if grades.get("step2", 0) <= 2:
                grades["step2"] = 2
    else:
        logdict["report"].append(' - content check: comparison of filtered second and one-minute: one of the data sets is not available')

    monthdict[resolution] = logdict
    results[month] = monthdict
    return results


def baseline_check(config, results, debug=False):
    """
    DESCRIPTION
         Baseline check reads BLV files from one-minute archive. It analyses variations within basevalue measurements
         for repeated measurements, checks for overall variations throughout the year and determines the difference
         between adopted and measured baselines. TODO jump analysis
    :param config:
    :param results:
    :return:
    """
    # check for one minute directory and read blv data
    if debug:
        print ("Running baseline check")

    if not results:
        results = { "report" : [],
                    "warnings" : [],
                    "errors" : [],
                    "temporaryminutedata" : DataStream(),
                    "temporaryseconddata" : DataStream(),
                    "grades" : { "step4" : 0
                                 }
                    }
    grades = results.get('grades',{})
    grades["step3"] = 0
    baselinecheck = {}
    mindict = results.get('minute-data-directory',{})
    baselinecheck["report"] = ["\n## 3. One-minute collection - baseline analysis\n"]
    if mindict and len(mindict.get('blvdatacheck',[])) > 0:
        # Do the test
        grades["step3"] = 1
        bll = mindict.get('blvdatacheck',[])
        basefile = bll[0]
        if len(bll) > 1:
            baselinecheck["report"].append(" - multiple BLV files contained in archive: selecting {}".format(basefile))
        else:
            baselinecheck["report"].append(" - selecting {} for baseline analysis".format(basefile))
        blvdata = read(basefile)
        baselinecheck["data"] = blvdata
        headx = blvdata.header.get("col-dx", "")
        heady = blvdata.header.get("col-dy", "")
        headz = blvdata.header.get("col-dz", "")
        headf = blvdata.header.get("col-df", "")
        unitx = blvdata.header.get("unit-col-dx", "")
        unity = blvdata.header.get("unit-col-dy", "")
        unitz = blvdata.header.get("unit-col-dz", "")
        unitf = blvdata.header.get("unit-col-df", "")
        # get average sampling rate
        means = blvdata.dailymeans(keys=['dx', 'dy', 'dz']) # lots of nan values in here - check
        srmeans = means.get_sampling_period()
        baselinecheck["report"].append(" - basevalues measured on average every {:.1f} days".format(srmeans/86400.))
        # Average and maximum standard deviation
        means = means._drop_nans('dx')
        # print ("means", means.mean('dx',percentage=1), means.amplitude('dx'))
        baselinecheck["report"].append(" - average deviation of repeated measurements is: {:.2f}{} for {}, {:.4f}{} for {} and {:.2f}{} for {}".format(
            means.mean('dx', percentage=1), unitx, headx, means.mean('dy', percentage=1), unity, heady,
            means.mean('dz', percentage=1), unitz, headz))
        if means.mean('dx', percentage=1) > 0.5 or means.mean('dz', percentage=1) > 0.5:
            baselinecheck["report"].append(" - minor variations between repeated basevalue measurements")
        if means.mean('dx', percentage=1) > 3.0 or means.mean('dz', percentage=1) > 3.0:
            results["warnings"].append('Baseline analysis: found relatively large variations between repeated basevalue measurements')
            if grades.get("step3") <= 2:
                grades["step3"] = 2
        fmeans = blvdata._drop_nans('df')
        baselinecheck["report"].append(" - average delta F mean for the whole year is: {:.2f}{}".format(
            fmeans.mean('df', percentage=1),unitf))

        # analyse residuum between baseline and basevalues
        func = blvdata.header.get('DataFunctionObject', [])
        if func:
            residual = blvdata.func2stream(func, mode='sub', keys=['dx', 'dy', 'dz'])
            resDIx, resDIstdx = residual.mean('dx', std=True, percentage=10)
            resDIy, resDIstdy = residual.mean('dy', std=True, percentage=10)
            resDIz, resDIstdz = residual.mean('dz', std=True, percentage=10)
            baselinecheck["report"].append(" - average residual between baseline and basevalues is: {}={:.3f}{}, {}={:.5f}{}, {}={:.3f}{}".format(
                headx, resDIx, unitx, heady, resDIy, unity, headz, resDIz, unitz))
            if resDIx > 0.1 or resDIz > 0.1:
                baselinecheck["report"].append(" - minor deviations between adopted baseline and basevalues")
            if resDIx > 0.5 or resDIz > 0.5:
                results["warnings"].append('Baseline analysis: large deviations between adopted baseline and basevalues')
                if grades.get("step3") <= 2:
                    grades["step3"] = 2
            # overall baseline variation
            # get maximum and minimum of the function for x and z
            amplitude = blvdata.func2stream(func, mode='values', keys=['dx', 'dy', 'dz'])
            ampx = amplitude.amplitude('dx')
            ampy = amplitude.amplitude('dy')
            ampz = amplitude.amplitude('dz')
            maxamp = np.max([ampx, ampy, ampz])  # Not conclusive if y contains declination
            baselinecheck["report"].append(" - maximum amplitude of baseline is {:.1f}{}".format(maxamp, unitx))
            # calculate the derivative of the amplitude - for jump evaluation TODO
            #damp = amplitude.derivative(keys=['dx','dy','dz'], put2keys=['x','y','z'])
            # get all indices of np.abs exceeding threshold
            #for key in ['x','y','z']:
            #print (damp)
            # PLEASE note: amplitude test is currently, effectively only testing x and z component
            #              this is still useful as maximum amplitudes are expected in these components
            if maxamp > 5:
                baselinecheck["report"].append(" - amplitude of adopted baseline exceeds INTERMAGNET threshold of 5 nT. "
                                               "Jumps ? ")
                baselinecheck["report"].append("   Evaluation of jumps will be part of a future version.")
        else:
            baselinecheck["report"] = [" - could not extract adopted baseline"]
    else:
        baselinecheck["report"] = [" - no baseline data available for checking"]

    results['baseline-analysis'] = baselinecheck
    return results


def header_test(config, results, debug=False):
    """
    DESCRIPTION
        Compare header information from different data sources (BLV, yearmean, datafiles) which are expected to be
        identical or similar.
    :param config:
    :param results:
    :return:
    """
    if debug:
        print ("Running header test")

    if not results:
        results = { "report" : [],
                    "warnings" : [],
                    "errors" : [],
                    "temporaryminutedata" : DataStream(),
                    "temporaryseconddata" : DataStream(),
                    "grades" : { "step4" : 0
                                 }
                    }
    grades = results.get('grades',{})
    grades["step4"] = 1
    blvdata = DataStream()
    yearmeandata = DataStream()
    headercheck = {}
    mindict = results.get('minute-data-directory',{})
    mindata = DataStream()
    minhmean = np.nan
    minfmean = np.nan
    blvhmean = np.nan
    blvfmean = np.nan
    yearhmean = np.nan
    yearfmean = np.nan

    headercheck["report"] = ["\n## 4. Testing consistency of header information in all files\n"]

    # read blv and yearmean data
    if len(mindict.get('blvdatacheck', [])) > 0:
        basefile = mindict.get('blvdatacheck', [])[0]
        blvdata = read(basefile)
    else:
        headercheck["report"].append(" - no baseline data")
    if len(mindict.get('yearmeancheck', [])) > 0:
        yearmean = mindict.get('yearmeancheck', [])[0]
        yearmeandata = read(yearmean)
    else:
        headercheck["report"].append(" - no yearmean data")

    ext = mindict.get('mindatacheck')
    path = config.get('mindatapath')
    if path and ext:
        mindata = read(os.path.join(path,"{}{}".format('*',ext)))
        # copy the full file to results
        results["temporaryminutedata"] =  mindata
        if mindata.header.get('DataComponents', '').lower().startswith('hdz'):
            mindata = mindata._convertstream('hdz2xyz')
        if len(mindata) >= 365*1440:
            minxmean = mindata.mean('x', percentage=50)
            minymean = mindata.mean('y', percentage=50)
            minzmean = mindata.mean('z', percentage=50)
            minhmean = np.sqrt(minxmean * minxmean + minymean * minymean)
            minfmean = np.sqrt(minxmean * minxmean + minymean * minymean + minzmean * minzmean)

    # Testing yearly means in BLV data, Minute data and
    if len(blvdata) > 0:
        blvhmean = blvdata.header.get('DataScaleX')
        blvfmean = blvdata.header.get('DataScaleZ')

    if len(yearmeandata) > 0:
        yearmeanx = yearmeandata.ndarray[1][-1]
        yearmeany = yearmeandata.ndarray[2][-1]
        yearmeanz = yearmeandata.ndarray[3][-1]
        yearhmean = np.sqrt(yearmeanx * yearmeanx + yearmeany * yearmeany)
        yearfmean = np.sqrt(yearmeanx * yearmeanx + yearmeany * yearmeany + yearmeanz * yearmeanz)
        headercheck["report"].append(" - yearmean file readable: contains data from {} to {}".format(yearmeandata.start().year,yearmeandata.end().year))

    if not all(np.isnan(np.array([yearhmean, blvhmean, minhmean]))):
        headercheck["report"].append(
            " - H means: BLV {:.1f}nT, {}: {:.1f}nT, YEARMEAN: {:.1f}nT".format(blvhmean, mindict.get("format", 'IAF'),
                                                                                minhmean, yearhmean))
        headercheck["report"].append(
            " - F means: BLV {:.1f}nT, {}: {:.1f}nT, YEARMEAN: {:.1f}nT".format(blvfmean, mindict.get("format", 'IAF'),
                                                                                minfmean, yearfmean))
        maxdiff = np.nanmax(np.abs(np.diff(np.array([yearhmean, blvhmean, minhmean]))))
        averagediff = np.nanmean(np.diff(np.array([yearhmean, blvhmean, minhmean])))
        if maxdiff >= 0.91 and averagediff >= 0.21: # diff between 20574.6 and 20575.5 is acceptable
            results["warnings"].append('Header analysis: significant differences in yearly mean values of BLV, YEARMEAN and One-minute data.')
            if grades.get("step4") <= 2:
                grades["step4"] = 2

    secdata = results.get("temporaryseconddata", DataStream())
    primaryheader = secdata.header
    if not primaryheader:
        primaryheader = mindata.header
    if primaryheader:
        excludelist = ['DataFormat', 'SensorID', 'DataComponents', 'DataSamplingRate', 'DataPublicationDate', 'col-f',
                       'col-x', 'col-y', 'col-z', 'col-df', 'unit-col-f', 'unit-col-x', 'unit-col-y', 'unit-col-z',
                       'unit-col-df', 'DataSamplingFilter', 'StationInstitution']
        for key in primaryheader:
            secvalue = str(secdata.header.get(key, ''))
            minvalue = str(mindata.header.get(key, ''))
            yearvalue = str(yearmeandata.header.get(key, ''))
            vals = np.array([secvalue, minvalue, yearvalue])
            rdecimals = 3
            if key in ['DataElevation']:
                rdecimals = 0
            cleanvals = [np.round(float(el),rdecimals) if methods.is_number(el) else el.lower().strip() for el in vals if not el == '']
            foundcount = len(cleanvals)
            if foundcount > 1 and not key.startswith('col') and not key.startswith('unit') and not key in excludelist:
                if cleanvals.count(cleanvals[0]) == len(cleanvals):
                    headercheck["report"].append(" - contents for {} are consistent".format(key.replace("Data",'').replace("Station",'')))
                else:
                    if key in ['DataAcquisitionLatitude', 'DataAcquisitionLongitude', 'DataElevation']:
                        results["warnings"].append('Header analysis: differences in header information for {}, {}'.format(key.replace("Data",'').replace("Station",''), vals))
                        if grades.get("step4") <= 2:
                            grades["step4"] = 2
                    else:
                        headercheck["report"].append(" - different header contents for {}, {}".format(key.replace("Data",'').replace("Station",''), vals))
    else:
        results["errors"].append('Header analysis: could neither access one-second nor one-minute header information')
        if grades.get("step4") <= 3:
            grades["step4"] = 3

    results['header-analysis'] = headercheck
    return results


def k_value_test(config, results, debug=False):
    """
    DESCRIPTION
        Compare header information from different data sources (BLV, yearmean, datafiles) which are expected to be
        identical or similar.
    :param config:
    :param results:
    :return:
    """
    if debug:
        print ("Running k value test")

    if not results:
        results = { "report" : [],
                    "warnings" : [],
                    "errors" : [],
                    "temporaryminutedata" : DataStream(),
                    "temporaryseconddata" : DataStream(),
                    "grades" : { "step5" : 0
                                 }
                    }
    grades = results.get('grades',{})
    grades["step5"] = 1
    korgdata = DataStream()
    kdkadata = DataStream()
    k_test = {}
    kdata_found = False
    korgvals = []

    k_test["report"] = ["\n## 5. Testing activity indices in files and comparing to K (FMI)\n"]

    mindict = results.get('minute-data-directory',{})
    k_test["data"] = DataStream()
    k_test["diffdata"] = DataStream()

    # get k vavlues form IAF data if available
    ext = mindict.get('mindatacheck')
    path = config.get('mindatapath')
    if path and ext and mindict.get('format') == 'IAF':
        korgdata = read(os.path.join(path,"{}{}".format('*',ext)), resolution='k')
        if debug:
            print ("HEADER", korgdata.header)
        korgvals = korgdata._get_column('var1')
        k_test["report"].append(" - found {} K values in IAF data set".format(len(korgvals)))
        if len(korgvals) > 0:
            k_test["data"] = korgdata
        kdata_found = True
    # get k values from dka if available
    dka = mindict.get('dkacheck',[])
    if len(dka) > 0:
        kdkadata = read(dka[0])
        k_test["report"].append(" - found {} K values in separate DKA file".format(len(kdkadata)))
        kdata_found = True
        if len(korgvals) > 0:
            kdiff = subtract_streams(korgdata, kdkadata)
            karr = np.nanmax(np.abs(np.diff(kdiff._get_column('var1'))))
            if karr < 0.1:
                k_test["report"].append(" - K values in IAF and DKA are identical")
            else:
                k_test["report"].append(" - K values in IAF and DKA are different. DKA is not obligatory but you should check that.")
    # calculate k values from IAF
    data = results.get("temporaryminutedata", DataStream())
    if len(data) > 0:
        from magpy.core import activity
        kdata = activity.K_fmi(data, K9_limit=data.header.get('StationK9', 500), longitude=data.header.get('DataAcquisitionLongitude', 15.0))
        kdata_found = True
        if len(korgvals) > 0:
            korgdata = korgdata.trim(kdata.start(), kdata.end()+timedelta(seconds=kdata.samplingrate()))
            if debug:
                print ("original data and calculated")
                print (len(korgdata), len(kdata))
                print (korgdata)
            kdiff = subtract_streams(korgdata, kdata)
            km, ks = kdiff.mean('var1', std=True, percentage=1)
            if debug:
                print ("K difference mean and std", km, ks)
            karr = np.nanmax(np.abs(np.diff(kdiff._get_column('var1'))))
            if karr > 1 and np.abs(km) > 0.05 and ks > 0.2: # arbitrary thresholds
                k_test["report"].append(
                    " - Reported K values in IAF and recalculated from FMI differ.")
                results["warnings"].append(
                    'K analysis: observed differences between K values and redetermined FMI K values .')
                grades["step5"] = 2
            elif karr <= 1 and np.abs(km) < 0.01 and ks < 0.1:
                k_test["report"].append(
                    " - Reported K values in IAF correspond very well to FMI calculation.")
            else:
                k_test["report"].append(
                    " - Reported K values in IAF are similar to FMI calculation.")
            k_test["diffdata"] = kdiff

    if not kdata_found:
        k_test["report"].append(" - no K values available.")

    results['k-value-analysis'] = k_test
    return results


if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: checkdata PACKAGE")
    print("THIS IS A TEST RUN OF THE MAGPY.OPT CHECKDATA PACKAGE.")
    print("All main methods will be tested. This may take a while.")
    print("If errors are encountered they will be listed at the end.")
    print("Otherwise True will be returned")
    print("----------------------------------------------------------")
    print()

    import subprocess
    config = {'mindatapath' : '/home/leon/GeoSphereCloud/Daten/CobsDaten/Yearbook2023/IAF', 'months' : [6]}
    #config = {'mindatapath' : '/home/leon/Tmp/CheckData/minute/LYC', 'secdatapath' : '/home/leon/Tmp/CheckData/second/LYC', 'months' : [6]}
    #config = {'mindatapath' : '/home/leon/Tmp/CheckData/minute/CNB', 'secdatapath' : '/home/leon/Tmp/CheckData/second/CNB', 'months' : [6]}
    results = {
        "report": "# Report of MagPys data checking tool box\n based on MagPy version {}\n".format(magpyversion),
        "warnings": [],
        "errors": [],
        "temporarydata": DataStream()
        }
    ok = True
    errors = {}
    successes = {}
    if ok:
        testrun = './testflagfile.json' # define a test file later on
        t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)
        while True:
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                results = check_minute_directory(config, results)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['check_minute_directory'] = ("Version: {}: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['check_minute_directory'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with check_minute_directory.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                results = check_second_directory(config, results)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['check_second_directory'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['check_second_directory'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with check_second_directory.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                """
                results = {'warning': [], 'errors': [], 'temporarydata': [],
                           'minute-data-directory': {'grade': 4, 'year': 2023, 'stationid': 'WIC',
                                                     'mindatacheck': '.BIN',
                                                     'blvdatacheck': [
                                                         '/home/leon/GeoSphereCloud/Daten/CobsDaten/Yearbook2023/IAF/WIC2023.blv'],
                                                     'yearmeancheck': [
                                                         '/home/leon/GeoSphereCloud/Daten/CobsDaten/Yearbook2023/IAF/YEARMEAN.WIC']
                                                     }
                           }
                """
                # requires results
                results = read_month(config, results, month=config.get('months')[0], debug=False)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['read_month'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['read_month'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with read_month.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                # requires results
                results = consistency_test(config, results, month=config.get('months')[0], debug=False)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['consistency_test'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['consistency_test'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with consistency_test.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                # requires results
                results = content_test(config, results, month=config.get('months')[0], debug=False)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['content_test'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['content_test'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with content_test.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                # requires results
                results = baseline_check(config, results, debug=True)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['baseline_check'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['baseline_check'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with baseline_check.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                # requires results
                results = header_test(config, results, debug=True)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['header_test'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['header_test'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with header_test.")
            results = k_value_test(config, results, debug=True)
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                # requires results
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['k_value_test'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['k_value_test'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with k_value_test.")

            # If end of routine is reached... break.
            break

        t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
        time_taken = t_end_test - t_start_test
        print(datetime.now(timezone.utc).replace(tzinfo=None), "- Checkdata runtime testing completed in {} s. Results below.".format(time_taken.total_seconds()))

        print()
        print("----------------------------------------------------------")
        del_test_files = 'rm {}*'.format(testrun)
        subprocess.call(del_test_files,shell=True)
        if errors == {}:
            print("0 errors! Great! :)")
        else:
            print(len(errors), "errors were found in the following functions:")
            print(" {}".format(errors.keys()))
            print()
            for item in errors:
                    print(item + " error string:")
                    print("    " + errors.get(item))
        print()
        print("Good-bye!")
        print("----------------------------------------------------------")
