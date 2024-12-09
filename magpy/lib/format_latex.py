"""
MagPy
LaTeX Table output filter
Written by Roman Leonhardt December 2012
- contains write function for hour data

ToDo: use longer files (analyze full year data if only monthly files are available
ToDo: add table footer

"""

import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import DataStream, read, merge_streams, subtract_streams, magpyversion
from datetime import datetime, timedelta, timezone
import os
import numpy as np
from magpy.opt.Table import Table
import logging
logger = logging.getLogger(__name__)
KEYLIST = DataStream().KEYLIST


def writeLATEX(datastream, filename, **kwargs):
    """
    Writing WDC format data.
    """

    mode = kwargs.get('mode')
    keys = kwargs.get('keys')

    if not keys:
        keys = ['x','y','z','f']
    header = datastream.header
    iagacode = header.get('StationIAGAcode',"").upper()
    caption = header.get('TEXcaption',"")
    label = header.get('TEXlabel',"")
    justs = header.get('TEXjusts',"") # e.g. 'lrccc'
    rotate = header.get('TEXrotate',False)
    tablewidth = header.get('TEXtablewidth',"")
    tablenum = header.get('TEXtablenum',"")
    fontsize = header.get('TEXfontsize',"")

    if not tablewidth:
        tablewidth = '0pt'
    if not fontsize:
        fontsize = '\\footnotesize'

    keylst = datastream._get_key_headers()
    if not 'x' in keylst or not 'y' in keylst or not 'z' in keylst:
        print("format_LATEX: writing tables requires at least x,y,z components")
        return False
    elif not 'f' in keylst:
        keys = ['x','y','z']

    ndtype = False
    if len(datastream.ndarray[0]) > 0:
        ndtype = True
    datalen = datastream.length()[0]

    if mode == 'wdc':
        print("formatLATEX: Writing wdc mode")
        # 1. determine sampling rate
        samplinginterval = datastream.get_sampling_period()/24/3600
        # get difference between first and last time in days
        ts,te = datastream._find_t_limits()
        deltat = te-ts
        #deltat = datastream[-1].time - datastream[0].time
        if 0.8 < samplinginterval/30.0 < 1.2:
            srange = 12
            sint = 'month'
            sintprev = 'year'
            datestr = '%Y'
            sideheadstr = ''
        elif 0.8 < samplinginterval < 1.2:
            srange = 31
            sint = 'day'
            sintprev = 'month'
            datestr = '%m'
            sideheadstr = '%Y'
        elif 0.8 < samplinginterval*24 < 1.2:
            srange = 24
            sint = 'hour'
            sintprev = 'day'
            datestr = '%b%d'
            sideheadstr = '%Y'
        elif 0.8 < samplinginterval*24*60 < 1.2:
            srange = 60
            sint = 'minute'
            sintprev = 'hour'
            datestr = '%H'
            sideheadstr = '%b%d %Y'
        elif 0.8 < samplinginterval*24*3600 < 1.2:
            srange = 60
            sint = 'second'
            sintprev = 'minute'
            datestr = '%H:%M'
            sideheadstr = '%b%d %Y'
        else:
            logging.error('Could not determine sampling rate for latex output: samplinginterval = %f days' % samplinginterval)
            return
        numcols = srange+1

        headline = np.array(range(srange+1)).tolist()
        headline[0] = sintprev
        headline.append('mean')
        if not justs:
            justs = 'p'*(numcols+1)

        fout = open(filename, "w", newline='')

        t = Table(numcols+1, justs=justs, caption=caption, label=label, tablewidth=tablewidth, tablenum=tablenum, fontsize=fontsize, rotate=True)
        t.add_header_row(headline)

        aarray = [[] for key in KEYLIST]
        barray = [[] for key in KEYLIST]
        for key in keys:
            pos = KEYLIST.index(key)
            aarray[pos] = np.empty((srange+1,int(np.round(float(datalen)/float(srange))),))
            aarray[pos][:] = np.nan
            aarray[pos] = aarray[pos].tolist()

            bar = datastream._get_column(key)
            bar = bar[~np.isnan(bar)]
            mbar = np.floor(np.min(bar)/100.)*100.

            if np.max(bar) - mbar < 1000:
                sigfigs = 3

            #for elem in datastream:
            for i in range(datalen):
                elem = datastream.ndarray[pos][i]
                timeval = datastream.ndarray[0][i]
                dateobj = timeval.replace(tzinfo=None)
                currx = eval('dateobj.'+sint) + 1
                curry = eval('dateobj.'+sintprev)-1
                datecnt = datetime.strftime(timeval.replace(tzinfo=None),datestr)
                aarray[pos][0][curry] = datecnt
                aarray[pos][currx][curry] = elem-mbar
                
                #exec('%sarray[0][curry] = datecnt' % key)
                #exec('%sarray[currx][curry] = elem%s-m%s' % (key,key,key))

            mecol = []
            #addcollist = eval(key+'array')
            addcollist = aarray[pos]
            tmpar = np.array(addcollist)
            tmpar = np.transpose(tmpar)
            for i in range(len(tmpar)):
                meanlst = []
                for j in range(len(addcollist)):
                    meanlst.append(addcollist[j][i])
                try:
                    if len(meanlst) > 24:
                        median = np.mean(meanlst[1:])
                    else:
                        median = np.nan
                except:
                    median = np.nan
                    pass
                mecol.append(median)
            addcollist.append(mecol)
            numcols = numcols+1

            label = datetime.strftime(timeval.replace(tzinfo=None),sideheadstr) + ', Field component: ' + key.upper() + ', Base: ' + str(mbar) + ', Unit: ' + datastream.header.get('unit-col-'+key,'')
            t.add_data(addcollist, label=label, labeltype='side', sigfigs=0)

        t.print_table(fout)
        fout.close()
        return True
    else:
        numcols = len(keys)+1
        if not justs:
            justs = 'l'*numcols
        headline = ['Date']
        samplinginterval = datastream.get_sampling_period()
        if 0.8 < samplinginterval/365.0 < 1.2:
            datestr = '%Y'
        elif 0.8 < samplinginterval/30.0 < 1.2:
            datestr = '%Y-%m'
        elif 0.8 < samplinginterval < 1.2:
            datestr = '%Y-%m-%d'
        elif 0.8 < samplinginterval*24 < 1.2:
            datestr = '%Y-%m-%dT%H:%M'
        else:
            datestr = '%Y-%m-%dT%H:%M:%S.%f'
        col1tmp = datastream._get_column('time')
        col1 = []
        for elem in col1tmp:
            col1.append(datetime.strftime(elem,datestr))
        addcollist = [col1]
        for iter,key in enumerate(keys):
            # extract headers
            colhead = header.get('col-'+key," ")
            if not header.get('unit-col-'+key,'') == '':
                colhead = colhead+' $['+header.get('unit-col-'+key,'')+']$'
            headline.append(colhead)
            # Extract data and time columns
            column = str(iter+2)
            exec('col'+ column + ' = datastream._get_column(\'' + key + '\')')
            addcollist.append(eval('col'+ column))

        if sys.version_info >= (3,0,0):
            fout = open(filename, "w", newline='')
        else:
            fout = open(filename, "wb")

        t = Table(numcols, justs=justs, caption=caption, label=label, tablewidth=tablewidth, tablenum=tablenum, fontsize=fontsize, rotate=rotate)
        t.add_header_row(headline)
        #col3 = [[0.12345,0.1],[0.12345,0.01],[0.12345,0.001]]
        t.add_data(addcollist, sigfigs=3)
        t.print_table(fout)
        fout.close()

        return True

if __name__ == '__main__':

    from scipy import signal
    import subprocess
    print()
    print("----------------------------------------------------------")
    print("TESTING: LATEX FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE LATEX LIBRARY.")
    print("All main methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()
    # 1. Creating a test data set of minute resolution and 1 month length
    #    This testdata set will then be transformed into appropriate output formats
    #    and written to a temporary folder by the respective methods. Afterwards it is
    #    reloaded and compared to the original data set
    def create_minteststream(startdate=datetime(2022, 11, 1), addnan=True):
        c = 1000  # 4000 nan values are filled at random places to get some significant data gaps
        l = 32 * 1440
        #import scipy
        teststream = DataStream()
        array = [[] for el in DataStream().KEYLIST]
        win = signal.windows.hann(60)
        a = np.random.uniform(20950, 21000, size=int(l / 2))
        b = np.random.uniform(20950, 21050, size=int(l / 2))
        x = signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        if addnan:
            x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
        array[1] = x[1440:-1440]
        a = np.random.uniform(1950, 2000, size=int(l / 2))
        b = np.random.uniform(1900, 2050, size=int(l / 2))
        y = signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        if addnan:
            y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
        array[2] = y[1440:-1440]
        a = np.random.uniform(44300, 44400, size=l)
        z = signal.convolve(a, win, mode='same') / sum(win)
        array[3] = z[1440:-1440]
        array[4] = np.sqrt((x * x) + (y * y) + (z * z))[1440:-1440]
        array[0] = np.asarray([startdate + timedelta(minutes=i) for i in range(0, len(array[1]))])
        teststream = DataStream(header={'SensorID': 'Test_0001_0001'}, ndarray=np.asarray(array, dtype=object))
        minstream = teststream.filter()
        teststream.header['col-x'] = 'X'
        teststream.header['col-y'] = 'Y'
        teststream.header['col-z'] = 'Z'
        teststream.header['col-f'] = 'F'
        teststream.header['unit-col-x'] = 'nT'
        teststream.header['unit-col-y'] = 'nT'
        teststream.header['unit-col-z'] = 'nT'
        teststream.header['unit-col-f'] = 'nT'
        teststream.header['StationID'] = 'XXX'
        teststream.header['StationIAGAcode'] = 'XXX'
        return teststream

    teststream = create_minteststream(addnan=False)
    teststream = teststream.trim('2022-11-22T14:00:00','2022-11-22T15:00:00')

    errors = {}
    successes = {}
    testrun = 'MAGPYTESTFILE'
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)

    while True:
        testset = 'LATEX-HOUR'
        try:
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            data = read('ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc')
            data.header[
                'TEXcaption'] = 'Hourly and daily means of field components X,Y,Z and independently measured F.'
            data.header['TEXlabel'] = 'hourlymean'
            data.write('/tmp', filenamebegins='hourlymean-', filenameends='.tex', keys=['x', 'y', 'z', 'f'], mode='wdc',
                       dateformat='%m', coverage='month', format_type='LATEX')
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))
        testset = 'LATEX-MIN'
        try:
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            teststream.header[
                'TEXcaption'] = 'Minute data.'
            teststream.header['TEXlabel'] = 'minutetest'
            teststream.header['TEXjusts'] = 'lrcl'
            teststream.header['TEXrotate'] = True
            teststream.header['TEXtablewidth'] = '10cm'
            teststream.header['TEXtablenum'] = '4.1'
            teststream.header['TEXfontsize'] = '10'
            teststream.write('/tmp', filenamebegins='mintest', filenameends='.tex', keys=['x', 'y', 'z'], mode='list',
                       coverage='all', format_type='LATEX')
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))

        break

    t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
    time_taken = t_end_test - t_start_test
    print(datetime.now(timezone.utc).replace(tzinfo=None), "- Stream testing completed in {} s. Results below.".format(time_taken.total_seconds()))

    print()
    print("----------------------------------------------------------")
    del_test_files = 'rm {}*'.format(os.path.join('/tmp',testrun))
    #subprocess.call(del_test_files,shell=True)
    for item in successes:
        print ("{} :     {}".format(item, successes.get(item)))
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
