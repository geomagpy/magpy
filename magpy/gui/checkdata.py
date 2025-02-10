# -*- coding: utf-8 -*-

"""
checkdata contains methods to support geomagnetic data checking

the following methods are contained:

|class | method | since version | until version | runtime test | result verifi. | manual | *used by       |
|----- | ------ | ------------- | ------------- | ------------ | -------------- | ------ | -------------- |
|**core.methods** |  |          |               |              |              |        |   |
|    | _delta_F_test    | 2.0.0 |               | yes          |              |        | consistency_test |
|    | check_minute_directory |  2.0.0 |        | yes          |              |        |   |
|    | convert_geo_coordinate | 2.0.0 |         | yes          |              |        |   |
|    | read_month       | 2.0.0 |               | yes          |              |        |   |
|    | consistency_test | 2.0.0 |               | yes          |              |        |   |
|    | content_test     | 2.0.0 |               | --           | --           |        |   |
|    | baseline_test    | 2.0.0 |               |              | yes          |        |   |
|    | header_test      | 2.0.0 |               |              | yes          |        |   |
|    | k_value_test     | 2.0.0 |               |              | yes          |        |   |
"""


import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2

import numpy as np
import os
from datetime import datetime, timedelta, timezone
from matplotlib.dates import date2num
import glob
from magpy.stream import read, DataStream, magpyversion, merge_streams, subtract_streams
from magpy.core import methods
from magpy.lib.magpy_formats import IAFBINMETA, IAGAMETA, IMAGCDFMETA


def _delta_F_test(fdata, debug=True):
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
    ftest = ftest._drop_nans(scal)
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

    fmean, fstd = ftest.mean('df',std=True)
    fmedian, fstd = ftest.mean('df',meanfunction='median',std=True)
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
    year = 1777
    grades = results.get('grades',{})
    res_min_dir = {}
    if grades.get("step1",0) <= 1:
        grades["step1"] = 1
    res_min_dir["report"] = ["### One-minute directory analysis"]
    res_min_dir["year"] = year
    res_min_dir["stationid"] = stationid
    res_min_dir["mindatacheck"] = ""
    res_min_dir["blvdatacheck"] = []
    res_min_dir["yearmeancheck"] = []

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
            if grades.get("step1") <= 2:
                grades["step1"] = 2
            results["errors"].append("Check minute directory: no baseline file")
        if minutesummary.get('dka',0) >= 1:
            res_min_dir["report"].append(" - found k-value DKA file")
        if minutesummary.get('png',0) >= 1:
            res_min_dir["report"].append(" - auxiliary files are present")

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
    res_sec_dir["report"] = ["### One-second directory analysis"]
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

        if secondsummary.get('cdf',0) >= 365:
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


def read_month(config, results, month=1, debug=True):
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
    for resolution in ["minute", "second"]:
        if debug:
            print ("Running for resolution", resolution)
        res_dir = results.get('{}-data-directory'.format(resolution))
        datapath = config.get('{}datapath'.format(resolution[:3]))
        logdict = {}
        logdict["report"] = ["#### One-{} read test".format(resolution)]
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
                    print (" - loading from directory:", os.path.join(datapath, "*"+ext))
                logdict['Data path'] = os.path.join(datapath, "*"+ext)
                data = read(os.path.join(datapath, "*"+ext), starttime=startdate, endtime=enddate)
                if not len(data) > 0:
                    # eventually start and endtime could not be identified in filename like for IAF, so read all and trim
                    data = read(os.path.join(datapath, "*" + ext))
                    data = data.trim(starttime=startdate, endtime=enddate)
                # get month and year
                if not len(data) > 0:
                    logdict["report"].append(" - data could not be read -aborting")
                    results["errors"].append("Read test: {} data could not be read for month {}".format(resolution, month))
                    if grades.get("step2") <= 3:
                        grades["step2"] = 3
                else:
                    results['temporary{}data'.format(resolution)] = data
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
                            if grades.get("step1") <= 2:
                                grades["step1"] = 2
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


def consistency_test(config, results, month=1, debug=True):
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
                    "grades" : { "step3" : 0
                                 }
                    }
    grades = results.get('grades',{})
    if grades.get("step3",0) <= 1:
        grades["step3"] = 1
    #res_cons_test = {}
    for resolution in ["minute", "second"]:
        if debug:
            print ("Running consistency test for resolution", resolution)
        monthdict = results.get(month)
        logdict = monthdict.get(resolution)
        data = results.get('temporary{}data'.format(resolution))
        if logdict and data:
            logdict["report"].append("#### One-{} consistency test".format(resolution))
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
                if len(vlen) > 0 and len(slen) > 0 and not vlen[0] == slen[0]:
                    logdict["report"].append(" - found different amounts of scalar data N={} and variometer data N={}".format(slen[0], vlen[0]))
                    logdict["report"].append("   filtering both data sets and merging at equal time steps before delta F analysis")
                    print (" extracting and filtering scalar data before delta F analysis")
                    scalardata = read(logdict.get("Data path"), starttime=logdict.get("Data limits")[0], endtime=logdict.get("Data limits")[1], select="scalar")
                    scalardata = scalardata.filter()
                    vectordata = data.filter()
                    data = merge_streams(vectordata,scalardata)
                    print ("HERE - filtering and merging")
                    logdict["filtered"] = data.copy()
            #if logdict.get("Data format").find("CDF") > -1:
            # read specific f data in case of IMAGCDF files
            # get data path and call command to read scalar data
            fdata = data.copy()
            fresult = _delta_F_test(fdata)
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
                    if grades.get("step3", 0) <= 2:
                        grades["step3"] = 2
                elif np.abs(fresult.get('dF mean')) >= 1.0:
                    results["warnings"].append("Consistency test: Month {}, {} resolution: average delta F differs strongly from zero".format(month,resolution))
                    if grades.get("step3", 0) <= 2:
                        grades["step3"] = 2
                elif np.abs(fresult.get('dF mean')) < 0.01 and np.abs(fresult.get('dF stddev')) < 0.01:
                    results["warnings"].append("Consistency test: Month {}, {} resolution: is F really independent from vector data?".format(month,resolution))
                    if grades.get("step3", 0) <= 2:
                        grades["step3"] = 2
                #print ("Diff between mean and median", np.abs(fresult.get('dF mean') - fresult.get('dF median')))
                if not np.isnan(fresult.get('dF mean')) and np.abs(fresult.get('dF mean') - fresult.get('dF median')) > 0.05:
                    results["warnings"].append("Consistency test: Month {}, {} resolution: Median and Mean are significantly different. Check outliers".format(month,resolution))
                    if grades.get("step3", 0) <= 2:
                        grades["step3"] = 2
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
                        if grades.get("step3", 0) <= 2:
                            grades["step3"] = 2
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
                        if grades.get("step3", 0) <= 2:
                            grades["step3"] = 2
                        hourwarn = True
                    if np.isnan(diffs.mean('y')) or np.abs(diffs.mean('y')) > 0.05:
                        results["warnings"].append(
                            "Consistency test: Month {}, {} resolution: IAF hour data differs from filtered minute data. Difference in Y={}".format(
                                month,resolution,diffs.mean('y')))
                        if grades.get("step3", 0) <= 2:
                            grades["step3"] = 2
                        hourwarn = True
                    if np.isnan(diffs.mean('z')) or np.abs(diffs.mean('z')) > 0.05:
                        results["warnings"].append(
                            "Consistency test: Month {}, {} resolution: IAF hour data differs from filtered minute data. Difference in Z={}".format(
                                month,resolution,diffs.mean('z')))
                        if grades.get("step3", 0) <= 2:
                            grades["step3"] = 2
                        hourwarn = True
                    if np.isnan(diffs.mean('df')) or np.abs(diffs.mean('df')) > 0.05:
                        # TODO if no df in hourly data than diff will np.nan - add that in the README
                        results["warnings"].append(
                            "Consistency test: Month {}, {} resolution: IAF hour data differs from filtered minute data. Difference in dF={}".format(
                                month,resolution,diffs.mean('df')))
                        hourwarn = True
                    if not hourwarn:
                        logdict["report"].append(
                            " - compared hourly data from IAF with filtered one-minute data: fine")
                else:
                    logdict["report"].append(
                    " - could not extract hourly data from IAF")
                    if grades.get("step3", 0) <= 3:
                        grades["step3"] = 3
                    results["errors"].append(
                        "Consistency test: Month {}, {} resolution: Could not extract IAF hourly data".format(
                            month, resolution))
            print ("Consistency", logdict)

        monthdict[resolution] = logdict
        print (monthdict.get(resolution))

    results[month] = monthdict

    return results


def content_test(config, results, month=1, debug=True):
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
                    "grades" : { "step4" : 0
                                 }
                    }
    grades = results.get('grades',{})
    if grades.get("step3",0) <= 1:
        grades["step3"] = 1
    # content test starts with one second
    resolution = "second"
    monthdict = results.get(month)
    logdict = monthdict.get(resolution)
    # check if filtered data is available already
    print (logdict)
    filtdata = logdict.get('filtered', DataStream())
    if not len(filtdata) > 0:
        data = results.get('temporary{}data'.format(resolution))
        if data and len(data) > 0:
            filtdata = data.filter()
    # get one-minute data:
    mindata = results.get('temporaryminutedata')
    if len(mindata) > 0 and len(filtdata) > 0:
        print ("Content test: Lengths of filtered data sets")
        print (len(mindata))
        print (len(filtdata)) # filtered data might be shorter than mindata because of filterwindow
        if len(mindata) == len(filtdata):
            # remove first and last time step
            diff = subtract_streams(filtdata, mindata, keys=['x', 'y', 'z'])
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
            print("  -> amplitudes determined")
            logdict['filtered vs minutedata: mean difference - x component'] = "{:.3} nT".format(xd)
            logdict['filtered vs minutedata: mean difference - y component'] = "{:.3} nT".format(yd)
            logdict['mean difference - z component'] = "{:.3} nT".format(zd)
            logdict['stddev of difference - x component'] = "{:.3} nT".format(xdst)
            logdict['stddev of difference - y component'] = "{:.3} nT".format(ydst)
            logdict['stddev of difference - z component'] = "{:.3} nT".format(zdst)
            logdict['amplitude of difference - x component'] = "{:.3} nT".format(xa)
            logdict['amplitude of difference - y component'] = "{:.3} nT".format(ya)
            logdict['amplitude of difference - z component'] = "{:.3} nT".format(za)
            if debug:
                print("  -> dictionary written")
            if max(xd, yd, zd) > 0.3:
                results["warnings"].append('Content check for month {}: one-minute and one-second data differ by more than 0.3 nT in monthly average'.format(month))
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
            if np.isnan(sum([xd, yd, zd, xa, ya, za])):
                logdict[
                    'report'].append(' - content check: not conclusive as NAN values are found')
            if debug:
                print("  -> one-minute comparison finished")
        else:
            results["warnings"].append('Comparison with definitive one-minute: filtered and original data sets differ in length')
    else:
        logdict["report"].append(' - content check: comparison of filtered second and one-minute: one of the data sets is not available')

    monthdict[resolution] = logdict
    results[month] = monthdict
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
    #config = {'mindatapath' : '/home/leon/GeoSphereCloud/Daten/CobsDaten/Yearbook2023/IAF', 'months' : [6]}
    #config = {'mindatapath' : '/home/leon/Tmp/CheckData/minute/LYC', 'secdatapath' : '/home/leon/Tmp/CheckData/second/LYC', 'months' : [6]}
    config = {'mindatapath' : '/home/leon/Tmp/CheckData/minute/CNB', 'secdatapath' : '/home/leon/Tmp/CheckData/second/CNB', 'months' : [6]}
    results = {
        "report": "## Report of MagPys data checking tool box\n based on MagPy version {}\n".format(magpyversion),
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
                #print ("MINUTE", result)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['check_minute_directory'] = ("Version: {}: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['check_minute_directory'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with check_minute_directory.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                results = check_second_directory(config, results)
                #print ("SECOND", result)
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
                results = read_month(config, results, month=config.get('months')[0], debug=True)
                print ("read DONE")
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['read_month'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['read_month'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with read_month.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                # requires results
                results = consistency_test(config, results, month=config.get('months')[0], debug=True)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['consistency_test'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['consistency_test'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with consistency_test.")
            results = content_test(config, results, month=config.get('months')[0], debug=True)
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                # requires results
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['content_test'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['content_test'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with content_test.")

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
