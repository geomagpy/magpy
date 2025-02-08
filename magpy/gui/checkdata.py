# -*- coding: utf-8 -*-

"""
checkdata contains methods to support geomagnetic data checking

the following methods are contained:

|class | method | since version | until version | runtime test | result verification | manual | *tested by |
|----- | ------ | ------------- | ------------- | ------------ | ------------------- | ------ | ---------- |
|**core.methods** |  |          |               |              |  |  | |
|    | check_minute_directory |  2.0.0 |              | yes           | yes          |        | |
|    | convert_geo_coordinate | 2.0.0 |        | yes           | yes          |        | |
|    | data_for_di     | 2.0.0 |               | yes*          | yes*         |        | absolutes |
|    | dates_to_url    | 2.0.0 |               |               | yes          |        | |
|    | deprecated      | 2.0.0 |               | --            | --           |        | |

"""

import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2

import numpy as np
import os
from datetime import datetime, timedelta, timezone
from matplotlib.dates import date2num
import glob
from magpy.stream import read, DataStream, magpyversion
from magpy.core import methods

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
    minutepath = config.get('mindatapath')
    months = config.get('months')
    stationid = "XXX"
    year = 1777
    res_min_dir = {}
    res_min_dir["grade"] = 1
    res_min_dir["report"] = ["### One-minute directory analysis"]
    res_min_dir["year"] = year
    res_min_dir["stationid"] = stationid
    res_min_dir["mindatacheck"] = ""
    res_min_dir["blvdatacheck"] = []
    res_min_dir["yearmeancheck"] = []

    if not minutepath:
        res_min_dir["report"].append(" no minute data path selected - skipping")
        res_min_dir["grade"] = 0
        results['minute-data-directory'] = res_min_dir
        return results
    elif not os.path.isdir(minutepath):
        # add some report to results
        res_min_dir["report"].append(" failed - could not access one-minute path: {}".format(minutepath))
        results["error"].append("Check minute directory: given data path not accessible")
        res_min_dir["grade"] = 6
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
                fname1 = fname.lower().replace(month.lower(), '')
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
        if minutesummary.get('bin') == 12 or minutesummary.get('min') >= 365 or minutesummary.get('cdf') >= 1:
            res_min_dir["report"].append(" - found correct number of data files")
            if minutesummary.get('bin') == 12:
                res_min_dir["format"] = "IAF"
            elif minutesummary.get('min') >= 365:
                res_min_dir["format"] = "IAGA-2002"
            elif minutesummary.get('cdf') >= 1:
                res_min_dir["format"] = "IMAGCDF"
        elif not minutesummary.get('bin') and not minutesummary.get('min') and not minutesummary.get('cdf'):
            res_min_dir["grade"] = 6
            results["error"].append("Check minute directory: no data files found")
        else:
            res_min_dir["grade"] = 4
            results["warning"].append("Check minute directory: incorrect amount of data files")
        if minutesummary.get('blv') >= 1:
            res_min_dir["grade"] = 4
            res_min_dir["report"].append(" - found baseline data file")
        else:
            res_min_dir["grade"] = 4
            results["error"].append("Check minute directory: no baseline file")
        if minutesummary.get('dka') >= 1:
            res_min_dir["report"].append(" - found k-value DKA file")
        if minutesummary.get('png') >= 1:
            res_min_dir["report"].append(" - auxiliary files are present")

    res_min_dir["year"] = year
    res_min_dir["stationid"] = stationid
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
    secondpath = config.get('secdatapath')
    months = config.get('months')
    stationid = "XXX"
    year = 1777
    res_sec_dir = {}
    res_sec_dir["grade"] = 1
    res_sec_dir["year"] = year
    res_sec_dir["report"] = ["### One-second directory analysis"]
    res_sec_dir["secdatacheck"] = ""

    if not secondpath:
        res_sec_dir["report"].append(" no second data path selected - skipping")
        res_sec_dir["grade"] = 0
        results['second-data-directory'] = res_sec_dir
        return results
    elif not os.path.isdir(secondpath):
        # add some report to results
        res_sec_dir["report"].append(" failed - could not access one-second path: {}".format(secondpath))
        results["error"].append("Check second directory: given data path not accessible")
        res_sec_dir["grade"] = 3
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
                            res_sec_dir["mindatacheck"].append(fn)

        if secondsummary.get('cdf') >= 365:
            res_sec_dir["report"].append(" - found correct number of ImagCDF data files")
            res_sec_dir["format"] = "IMAGCDF"
        elif secondsummary.get('sec') >= 365:
            res_sec_dir["grade"] = 1
            res_sec_dir["format"] = "IAGA-2002"
            res_sec_dir["report"].append(" - found correct number of IAGA-2002 data files")
        elif not secondsummary.get('cdf') and not secondsummary.get('sec'):
            res_sec_dir["grade"] = 3
            results["error"].append("Check second directory: no data files found")
        else:
            res_sec_dir["grade"] = 2
            results["warning"].append("Check second directory: incorrect amount of data files")

    res_sec_dir["year"] = year
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

    res_read_test = {}
    for resolution in ["minute", "second"]:
        if debug:
            print ("Running for resolution", resolution)
        res_dir = results.get('{}-data-directory'.format(resolution))
        datapath = config.get('{}datapath'.format(resolution[:3]))
        logdict = {}
        logdict["report"] = ["### One-{} read test".format(resolution)]
        logdict["grade"] = 0
        if res_dir:
            if debug:
                print(" - {} available: continuing".format(resolution))
            year = res_dir.get('year')
            ext = res_dir.get("{}datacheck".format(resolution[:3]))
            logdict["grade"] = 1
            # get time range to check
            startdate = datetime(year,month,1)
            enddate = startdate+timedelta(days=32)
            enddate = datetime(enddate.year, enddate.month, 1)
            if ext:  # found data
                if debug:
                    print (" - loading from directory:", os.path.join(datapath, "*"+ext))
                data = read(os.path.join(datapath, "*"+ext), starttime=startdate, endtime=enddate)
                if not len(data) > 0:
                    # eventually start and endtime could not be identified in filename like for IAF, so read all and trim
                    data = read(os.path.join(datapath, "*" + ext))
                    data = data.trim(starttime=startdate, endtime=enddate)
                # get month and year
                if not len(data) > 0:
                    logdict["report"].append(" - data could not be read -aborting")
                    results["errors"].append("Read test: {} data could not be read for month {}".format(resolution, month))
                    logdict["grade"] = 3
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
                        logdict["grade"] = 2
                    if (effectivedays * 24 * 3600) < cntafter:
                        logdict["report"].append(' - more data than expected: check data files for duplicates and correct coverage')
                        logdict["grade"] = 2
                    if not int(sr) == expectedsr:
                        logdict["report"].append(' - found sampling rate of {} sec, expected is {} sec'.format(sr, expectedsr))
                        logdict["grade"] = 3

                    dataformat = data.header.get('DataFormat')
                    logdict['Data format'] = dataformat
                    if debug:
                        print (" - logdict looks like", logdict)
                    """
                    if dataformat == 'IMAGCDF': # and sr==1:
                        referencemeta = IMAGCDFMETA
                    elif dataformat == 'IAGA':
                        referencemeta = IAGAMETA
                    elif dataformat == 'IAF':
                        referencemeta = IAFMETA
                    else:
                        pass
                    """
                        # checking header
                    headfailure = False
                    """
                    for head in referencemeta:
                        value = data.header.get(head, '')
                        if value == '':
                            results["warnings"].append("Simple read test:  no meta information for {}".format(head))
                            headfailure = True
                            res_read_test["grade"] = 2
                    if not headfailure:
                        res_read_test["report"].append(" - all header elements present - did not check contents however")

                    logdict['Leap second update'] = data.header.get('DataLeapSecondUpdated')
                    if not str(latestleapsecond) == str(data.header.get('DataLeapSecondUpdated')):
                        warningdict['Leap second'] = 'Leap second table seems to be outdated - please check'


                    """

        res_read_test[resolution] = logdict

        if debug:
            print (" ... resolution {} run done".format(resolution))
        #if seconddata == 'cdf':
        #    META = IMAGCDFMETA
        #elif seconddata == 'iaga':
        #    META = IAGAMETA


    results[month] = res_read_test

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
    results = {
        "report": "## Report of MagPys data checking tool box\n based on MagPy version {}\n".format(magpyversion),
        "warning": [],
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
            res = check_minute_directory(config, results)
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                res = check_minute_directory(config, results)
                #print ("MINUTE", res)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['check_minute_directory'] = ("Version: {}: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['check_minute_directory'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with check_minute_directory.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                #res = check_second_directory(config, results)
                #print ("SECOND", res)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['check_second_directory'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['check_second_directory'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with check_second_directory.")

            results = {'warning': [], 'errors': [], 'temporarydata': [],
                           'minute-data-directory': {'grade': 4, 'year': 2023, 'stationid': 'WIC',
                                                     'mindatacheck': '.BIN',
                                                     'blvdatacheck': [
                                                         '/home/leon/GeoSphereCloud/Daten/CobsDaten/Yearbook2023/IAF/WIC2023.blv'],
                                                     'yearmeancheck': [
                                                         '/home/leon/GeoSphereCloud/Daten/CobsDaten/Yearbook2023/IAF/YEARMEAN.WIC']
                                                     }
                           }
                # requires results
            res = read_month(config, results,  month=1)
            print(res)

            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                results = {'warning': [], 'errors': [], 'temporarydata': [],
                           'minute-data-directory': {'grade': 4, 'year': 2023, 'stationid': 'WIC',
                                                     'mindatacheck': '.BIN',
                                                     'blvdatacheck': [
                                                         '/home/leon/GeoSphereCloud/Daten/CobsDaten/Yearbook2023/IAF/WIC2023.blv'],
                                                     'yearmeancheck': [
                                                         '/home/leon/GeoSphereCloud/Daten/CobsDaten/Yearbook2023/IAF/YEARMEAN.WIC']
                                                     }
                           }
                # requires results
                res = read_month(config, results, month=1, debug=True)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['read_month_minute'] = (
                    "Version: {}: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['read_month_minute'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with read_month_minute.")

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
