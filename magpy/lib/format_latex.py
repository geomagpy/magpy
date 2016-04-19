"""
MagPy
LaTeX Table output filter
Written by Roman Leonhardt December 2012
- contains write function for hour data

ToDo: use longer files (anaylsze full year data if only monthly files are available
ToDo: add table footer

"""
from __future__ import print_function

from magpy.stream import *
from magpy.opt.Table import Table

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
        print("formatWDC: writing WDC data requires at least x,y,z components")
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
        samplinginterval = datastream.get_sampling_period()
        # get difference between first and last time in days
        ts,te = datastream._find_t_limits()
        deltat = date2num(te)-date2num(ts)
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

        fout = open( filename, "wb" )
        t = Table(numcols+1, justs=justs, caption=caption, label=label, tablewidth=tablewidth, tablenum=tablenum, fontsize=fontsize, rotate=True)
        t.add_header_row(headline)

        aarray = [[] for key in KEYLIST]
        barray = [[] for key in KEYLIST]
        for key in keys:
            pos = KEYLIST.index(key)
            aarray[pos] = np.empty((srange+1,int(np.round(float(datalen)/float(srange))),))
            aarray[pos][:] = np.nan
            aarray[pos] = aarray[pos].tolist()
            #aarray = list(aarray)

            bar = datastream.ndarray[pos]
            bar = bar[~np.isnan(bar)]
            mbar = np.floor(np.min(bar)/100.)*100.

            if np.max(bar) - mbar < 1000:
                sigfigs = 3

            """
            # exec('...' % key)
            # here starts the key dependend analysis
            exec('%sarray = np.empty((srange+1,int(np.round(float(datalen)/float(srange))),))' % key)
            exec('%sarray[:] = np.NAN' % key)
            exec('%sarray = %sarray.tolist()' % (key,key)) # using list, so that strings can be used
            # get means and variation:
            if ndtype:
                ind = KEYLIST.index(key)
                ar = datastream.ndarray[ind]
                exec('%sar = ar' % key)
            else:
                exec('%sar = np.array([elem.%s for elem in datastream if not isnan(elem.%s)])' % (key,key,key))
            exec('m%s = np.floor(np.min(%sar)/100)*100' % (key,key))
            if np.max(eval(key+'ar')) - eval('m'+key) < 1000:
                sigfigs = 3
            """

            #for elem in datastream:
            for i in range(datalen):
                if not ndtype:
                    elem = datastream[i]
                    elemx = elem.x
                    elemy = elem.y
                    elemz = elem.z
                    elemf = elem.f
                    timeval = elem.time
                else:
                    elem = datastream.ndarray[pos][i]
                    """
                    elemx = datastream.ndarray[1][i]
                    elemy = datastream.ndarray[2][i]
                    elemz = datastream.ndarray[3][i]
                    elemf = datastream.ndarray[4][i]
                    """
                    timeval = datastream.ndarray[0][i]
                dateobj = num2date(timeval).replace(tzinfo=None)
                currx = eval('dateobj.'+sint) + 1
                curry = eval('dateobj.'+sintprev)-1
                datecnt = datetime.strftime(num2date(timeval).replace(tzinfo=None),datestr)
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
                        median = float(NaN)
                except:
                    median = float(NaN)
                    pass
                mecol.append(median)
            addcollist.append(mecol)
            numcols = numcols+1

            label = datetime.strftime(num2date(timeval).replace(tzinfo=None),sideheadstr) + ', Field component: ' + key.upper() + ', Base: ' + str(mbar) + ', Unit: ' + datastream.header.get('unit-col-'+key,'')
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
            col1.append(datetime.strftime(num2date(elem),datestr))
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

        fout = open( filename, "wb" )
        t = Table(numcols, justs=justs, caption=caption, label=label, tablewidth=tablewidth, tablenum=tablenum, fontsize=fontsize, rotate=rotate)
        t.add_header_row(headline)
        #col3 = [[0.12345,0.1],[0.12345,0.01],[0.12345,0.001]]
        t.add_data(addcollist, sigfigs=3)
        t.print_table(fout)
        fout.close()

        return True
