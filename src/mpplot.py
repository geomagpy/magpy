#!/usr/bin/env python
"""
MagPy-Plotting: Functions for pretty plotting

Written by Rachel Bailey, 8th May 2014
"""

from stream import *

# TODO: Add in general plot function so that it works with same variables
#	as original plot function.

colorlist =  ['b','g','m','c','y','k','b','g','m','c','y','k']
symbollist = ['-','-','-','-','-','-','-','-','-','-','-','-']

def ploteasy(stream):
    #import plot TODO
    keys = stream._get_key_headers()
    sensorid = stream.header['SensorID']
    datadate = datetime.strftime(num2date(stream[0].time),'%Y-%m-%d')
    plottitle = "%s (%s)" % (sensorid,datadate)
    stream.plot(keys,
		fullday = True,
		confinex = True,
                plottitle = plottitle)

def plot_new(stream,variables,specialdict={},errorbars=False,padding=0,
	annotate=False,colorlist=colorlist,symbollist=symbollist,
	**kwargs):
    '''
    DEFINITION:
        This function creates a graph from a single stream.

    PARAMETERS:
    Variables:
        - stream:	(DataStream object) Stream to plot
	- variables:	(list) List of variables to plot.
    Kwargs:
	- annotate:	(bool/list=False) If True, will annotate plot.
	- bartrange:	(float) Variable for plotting of bars.
	- bgcolor:	(color='white') Background colour of plot.
	- colorlist:	(list(colors)) List of colours to plot with.
			Default = ['b','g','m','c','y','k','b','g','m','c','y','k']
	- confinex:	(bool=False) x-axis will be confined to smaller t-values if True.
	- errorbars:	(bool/list=False) If True, will plot corresponding errorbars:
			[ [False], [True], [False, False] ]
	- fmt:		(str) Format of outfile.
	- fullday:	(bool=False) Will plot fullday if True.
	- function:	(func) [0] is a dictionary containing keys (e.g. fx), 
			[1] the startvalue, [2] the endvalue
			Plot the content of function within the plot.
			CAUTION: Not yet implemented.
	- grid:		(bool=True) If True, will plot grid.
	- gridcolor:	(color='#316931') Colour of grid.
	- includeid:	(bool) If True, sensor IDs will be extracted from header data and
			plotted alongside corresponding data. Default=False
	- labelcolor:	(color='0.2') Colour of labels.
	- outfile:	(str) Path of file to plot figure to.
        - padding: 	(float/list) Float or list of padding for each variable.
	- plottitle:	(str) Title to put at top of plot.
	- plottype:	(NumPy str='discontinuous') Can also be 'continuous'.
	- savedpi:	(float=80) Determines dpi of outfile.
	- specialdict:	(dictionary) contains special information for specific plots.
			key corresponds to the column
			input is a list with the following parameters
			{'x':[ymin,ymax]}
	- symbollist:	(list) List of symbols to plot with. Default= '-' for all.

    RETURNS:
        - plot: 	(Pyplot plot) Returns plot as plt.show or savedfile
			if outfile is specified.

    EXAMPLE:
        >>> 

    APPLICATION:
        
    '''

    num_of_var = len(variables)
    if num_of_var > 9:
        loggerplot.error("plot: Can't plot more than 9 variables, sorry.")
	raise Exception("Can't plot more than 9 variables!")

    if len(symbollist) < num_of_var:
        loggerplot.error("plot: Length of symbol list does not match number of variables.")
	raise Exception("Length of symbol list does not match number of variables.")
    if len(colorlist) < num_of_var:
        loggerplot.error("plot: Length of color list does not match number of variables.")
	raise Exception("Length of color list does not match number of variables.")

    plot_dict = []
    count = 0

    for i in range(num_of_var):
        t = np.asarray([row[0] for row in stream])
        key = variables[i]
        if not key in KEYLIST[1:16]:
            loggerplot.error("plot: Column key (%s) not valid!" % key)
            raise Exception("Column key (%s) not valid!" % key)
        data_dict = {}
        ind = KEYLIST.index(key)
        y = np.asarray([row[ind] for row in stream])
        data_dict['tdata'] = t
        data_dict['ydata'] = y
        data_dict['color'] = colorlist[i]
        data_dict['symbol'] = symbollist[i]

        if padding:
            if type(padding) == list:
                ypadding = padding[i]
            else:
                ypadding = padding
        else:
            ypadding = 0

        if key in specialdict:
            specialparams = specialdict[key]
            data_dict['ymin'] = specialparams[0] - ypadding
            data_dict['ymax'] = specialparams[1] + ypadding
        else:
            data_dict['ymin'] = np.min(y) - ypadding
            data_dict['ymax'] = np.max(y) + ypadding

        # Define y-labels:
        try:
            ylabel = stream.header['col-'+key].upper()
        except:
            ylabel = ''
        try:
            yunit = stream.header['unit-col-'+key]
        except:
            yunit = ''
        if not yunit == '': 
            yunit = re.sub('[#$%&~_^\{}]', '', yunit)
            label = ylabel+' $['+yunit+']$'
        else:
            label = ylabel
        data_dict['ylabel'] = label

        if errorbars:
            if type(errorbars) == list:
                if errorbars[i]:
                    ind = KEYLIST.index('d'+key)
                    errors = np.asarray([row[ind] for row in stream])
                    if len(errors) > 0: 
                        data_dict['errors'] = errors
                    else:
                        loggerplot.warning("plot: No errors for key %s. Leaving empty." % key)
            else:
                ind = KEYLIST.index('d'+key)
                errors = np.asarray([row[ind] for row in stream])
                if len(errors) > 0: 
                    data_dict['errors'] = errors
                else:
                    loggerplot.warning("plot: No errors for key %s. Leaving empty." % key)

        if annotate:
	    if type(annotate) == list:
                if annotate[i]:
                    flag = stream._get_column('flag')
                    comments = stream._get_column('comment')
                    flags = array([flag,comments])
		    data_dict['annotate'] = True
                    data_dict['flags'] = flags
                else:
                    data_dict['annotate'] = False
            else:
                flag = stream._get_column('flag')
                comments = stream._get_column('comment')
                flags = array([flag,comments])
		data_dict['annotate'] = True
                data_dict['flags'] = flags
        else:
            data_dict['annotate'] = False

        plot_dict.append(data_dict)

    loggerplot.info("plot: Starting plotting function...")
    _plot(plot_dict, **kwargs)
    loggerplot.info("plot: Plotting completed.")


def plotStreams(streamlist,variables,padding=[],specialdict=[],errorbars=[],
	colorlist=colorlist,symbollist=symbollist,
	annotate=[],includeid=False,**kwargs):
    '''
    DEFINITION:
        This function plots multiple streams in one plot for easy comparison.

    PARAMETERS:
    Variables:
        - streamlist: 	(list(list)) A list containing the stream and the 
			variables from each stream to be plotted in a list, e.g.:
			[ stream1, stream2, etc...]
			[ fge, pos1, env1 ]
	- variables:	(list(list)) List containing the variables to be plotted
			from each stream, e.g:
			[ ['x'], ['f'], ['t1', 't2'] ]
    Args:
	LISTED VARIABLES:
	(NOTE: All listed variables must correspond in size to streamlist.)
	- annotate:	(list(bool)) If True, will annotate plot with flags, e.g.:
			[ [True], [True], [False, False] ]
	- errorbars:	(list(bool)) If True, will plot corresponding errorbars:
			[ [False], [True], [False, False] ]
        - padding: 	(list(list)) List of lists containing paddings for each
			respective variable, e.g:
			[ [5], [5], [0.1, 0.2] ]
	- specialdict:	(list(dict)) Same as plot variable, e.g:
			[ {'z': [100,150]}, {}, {'t1':[7,8]} ]
	NORMAL VARIABLES:
	- bartrange:	(float) Variable for plotting of bars.
	- bgcolor:	(color='white') Background colour of plot.
	- colorlist:	(list(colors)) List of colours to plot with.
			Default = ['b','g','m','c','y','k','b','g','m','c','y','k']
	- confinex:	(bool=False) x-axis will be confined to smaller t-values if True.
	- fmt:		(str) Format of outfile.
	- fullday:	(bool=False) Will plot fullday if True.
	- function:	(func) [0] is a dictionary containing keys (e.g. fx), 
			[1] the startvalue, [2] the endvalue
			Plot the content of function within the plot.
			CAUTION: Not yet implemented.
	- grid:		(bool=True) If True, will plot grid.
	- gridcolor:	(color='#316931') Colour of grid.
	- includeid:	(bool) If True, sensor IDs will be extracted from header data and
			plotted alongside corresponding data. Default=False
	- labelcolor:	(color='0.2') Colour of labels.
	- outfile:	(str) Path of file to plot figure to.
	- plottitle:	(str) Title to put at top of plot.
	- plottype:	(NumPy str='discontinuous') Can also be 'continuous'.
	- savedpi:	(float=80) Determines dpi of outfile.
	- symbollist:	(list) List of symbols to plot with. Default= '-' for all.

    RETURNS:
        - plot: 	(Pyplot plot) Returns plot as plt.show or saved file
			if outfile is specified.

    EXAMPLE:
        >>> plotStreams(streamlist, padding=padding, includeid=True, outfile='plots.png')

    APPLICATION:
        fge_file = fge_id + '_' + date + '.cdf'
        pos_file = pos_id + '_' + date + '.bin'
        lemi025_file = lemi025_id + '_' + date + '.bin'
        cs_file = cs_id + '_' + date + '.bin'
        fge = read(fge_file)
        pos = read(pos_file)
        lemi025 = read(lemi025_file,tenHz=True)
        cs = read(cs_file)
        streamlist = 	[ fge,			cs,			lemi025,	pos		]
	variables =	[ ['x','y','z'],	['f'],			['z'],		['f'] 		]
        specialdict = 	[ {}, 			{'f':[48413,48414]},	{},		{}		]
        errorbars =	[ [False,False,False],	[False],		[False],	[True]		]
        padding = 	[ [1,1,1], 		[1],			[1] ,		[1]		]
        annotate = 	[ [False,False,False],	[True],			[True] ,	[True]		]
        plotStreams(streamlist, variables, padding=padding,specialdict=specialdict,
		annotate=annotate,includeid=True,errorbars=errorbars,
		outfile='plots/all_magn_cut1.png',
		plottitle="WIC: All Magnetometers (%s)" % date)
    '''

    num_of_var = 0
    for item in variables:
        num_of_var += len(item)
    if num_of_var > 9:
        loggerplot.error("plotStreams: Can't plot more than 9 variables, sorry.")
	raise Exception("Can't plot more than 9 variables!")

    if len(symbollist) < num_of_var:
        loggerplot.error("plotStreams: Length of symbol list does not match number of variables.")
	raise Exception("Length of symbol list does not match number of variables.")
    if len(colorlist) < num_of_var:
        loggerplot.error("plotStreams: Length of color list does not match number of variables.")
	raise Exception("Length of color list does not match number of variables.")

    plot_dict = []
    count = 0

    for i in range(len(streamlist)):
        stream = streamlist[i]
        t = np.asarray([row[0] for row in stream])
        for j in range(len(variables[i])):
            data_dict = {}
            key = variables[i][j]
            if not key in KEYLIST[1:16]:
                loggerplot.error("plot: Column key (%s) not valid!" % key)
                raise Exception("Column key (%s) not valid!" % key)
            ind = KEYLIST.index(key)
            y = np.asarray([row[ind] for row in stream])
            data_dict['tdata'] = t
            data_dict['ydata'] = y
            data_dict['color'] = colorlist[count]
            data_dict['symbol'] = symbollist[count]

            if padding:
                ypadding = padding[i][j]
            else:
                ypadding = 0

            if specialdict:
                if key in specialdict[i]:
                    specialparams = specialdict[i][key]
                    data_dict['ymin'] = specialparams[0] - ypadding
                    data_dict['ymax'] = specialparams[1] + ypadding
                else:
                    data_dict['ymin'] = np.min(y) - ypadding
                    data_dict['ymax'] = np.max(y) + ypadding
            else:
                data_dict['ymin'] = np.min(y) - ypadding
                data_dict['ymax'] = np.max(y) + ypadding

            # Define y-labels:
            try:
                ylabel = stream.header['col-'+key].upper()
            except:
                ylabel = ''
                pass
            try:
                yunit = stream.header['unit-col-'+key]
            except:
                yunit = ''
                pass
            if not yunit == '': 
                yunit = re.sub('[#$%&~_^\{}]', '', yunit)
                label = ylabel+' $['+yunit+']$'
            else:
                label = ylabel
            data_dict['ylabel'] = label

            if errorbars:
                if errorbars[i][j]:
                    ind = KEYLIST.index('d'+key)
                    errors = np.asarray([row[ind] for row in stream])
                    if len(errors) > 0: 
                        data_dict['errors'] = errors
                    else:
                        loggerplot.warning("plotStreams: No errors for key %s. Leaving empty." % key)

            if annotate:
                if annotate[i][j]:
                    flag = stream._get_column('flag')
                    comments = stream._get_column('comment')
                    flags = array([flag,comments])
		    data_dict['annotate'] = True
                    data_dict['flags'] = flags
                else:
                    data_dict['annotate'] = False
            else:
                data_dict['annotate'] = False

            if includeid:
                sensor_id = stream.header['SensorID']
                data_dict['includeid'] = sensor_id

            plot_dict.append(data_dict)
            count += 1

    loggerplot.info("plotStreams: Starting plotting function...")
    _plot(plot_dict, **kwargs)
    loggerplot.info("plotStreams: Plotting completed.")


def _plot(data,savedpi=80,grid=True,gridcolor='#316931',
	bgcolor='white',plottitle=None,fullday=False,bartrange=0.06,
	labelcolor='0.2',confinex=False,outfile=None,
	fmt=None,plottype='discontinuous',**kwargs):
    '''
    For internal use only. Feed a list of dictionaries in here to plot.
    Every dictionary should contain all data needed for one single subplot.
    DICTIONARY OF EVERY SUBPLOT:
    [ { ***REQUIRED***
	'tdata' : t		(np.ndarray) Time
	'ydata' : y		(np.ndarray) Data y(t)
	'ymin'  : ymin		(float)	   Minimum of y-axis
	'ymax'  : ymax		(float)    Maximum of y-axis
	'symbol': '-'		(str)	   Symbol for plotting, '-' = line
	'color' : 'b'		(str)	   Colour of plotted line
	'ylabel': 'F [nt]'	(str)	   Label on y-axis
	'annotate': False	(bool)	   If this is True, must have 'flags' key

	OPTIONAL:
	'errorbars': eb		(np.ndarray) Errorbars to plot in subplot	
	'flags' : flags		(np.ndarray) Flags to add into subplot.
				Note: must be 2-dimensional, flags & comments.
        'includeid': LEMI025	(str) String pulled from header data. If available,
				will be plotted alongside data for clarity.
	'function': fn		(function object) Plot a function within the subplot.
	} ,

      {	'tdata' : ...				} ... ]

    GENERAL VARIABLES:
    plottitle = "Data from 2014-05-02"
    confinex = False
    bgcolor = 'blue'
    etc. ...
    '''

    # CREATE MATPLOTLIB FIGURE OBJECT:
    fig = plt.figure()
    plt_fmt = ScalarFormatter(useOffset=False)
    n_subplots = len(data)

    for i in range(n_subplots):

        subplt = "%d%d%d" %(n_subplots,1,i+1)

	#------------------------------------------------------------
	# PART 1: Dealing with data
	#------------------------------------------------------------

        # DEFINE DATA:
        t = data[i]['tdata']
        y = data[i]['ydata']
        color = data[i]['color']
        symbol = data[i]['symbol']

        # Deal with discontinuities:
        # TODO : how to deal with this?
        '''
        if plottype == 'discontinuous':
            y = self._maskNAN(y)
        else: 
            nans, test = self._nan_helper(y)
            newt = [t[idx] for idx, el in enumerate(y) if not nans[idx]]
            t = newt
            y = [el for idx, el in enumerate(y) if not nans[idx]]
        '''

        # CREATE SUBPLOT OBJECT & ADD TITLE:
        if i == 0:
            ax = fig.add_subplot(subplt, axisbg=bgcolor)
            if plottitle:
                ax.set_title(plottitle)
            a = ax
        else:
            ax = fig.add_subplot(subplt, sharex=a, axisbg=bgcolor)

        # PLOT DATA:
        # --> If bars are in the data (for e.g. k-index):
        if symbol == 'z':
            xy = range(9)
            for num in range(len(t)):
                if bartrange < t[num] < np.max(t)-bartrange:
                    ax.fill([t[num]-bartrange,t[num]+bartrange,t[num]+bartrange,t[num]-
				bartrange],[0,0,yplt[num]+0.1,yplt[num]+0.1],
				facecolor=cm.RdYlGn((9-yplt[num])/9.,1),alpha=1,edgecolor='k')
            ax.plot_date(t,y,color+'|')

        # --> Otherwise plot as normal:
        else:
            ax.plot_date(t,y,color+symbol)

        # PLOT ERROR BARS (if available):
        if 'errors' in data[i]:
            errorbars = data[i]['errors']
            ax.errorbar(t,y,yerr=errorbars,fmt=color+'o')

        # ANNOTATE:
        if data[i]['annotate'] == True:
            flags = data[i]['flags']
            emptycomment = "-"
            for idx, elem in enumerate(flags[1]):
                indexflag = int(flags[0][idx][4])
                if not elem == emptycomment:# and indexflag in ['0','1','3']:
                    ax.annotate(r'%s' % (elem),
                                xy=(t[idx], y[idx]),
                                xycoords='data', xytext=(20, 20),
                                textcoords='offset points',
                                bbox=dict(boxstyle="round", fc="0.8"),
                                arrowprops=dict(arrowstyle="->",
                                shrinkA=0, shrinkB=1,
                                connectionstyle="angle,angleA=0,angleB=90,rad=10"))

	# PLOT A GIVEN FUNCTION:     
	# TODO: This doesn't work yet 
        if 'function' in data[i]:
            fkey = 'f'+key
            function = data[i]['function']
            if fkey in function[0]:
		# --> Get the minimum and maximum relative times
                ttmp = arange(0,1,0.0001)
                ax.plot_date(self._denormalize(ttmp,function[1],function[2]),function[0][fkey](ttmp),'r-')

	#------------------------------------------------------------
	# PART 2: Formatting the plot
	#------------------------------------------------------------

        # ADD SENSOR IDS TO DATA PLOTS:
	if 'includeid' in data[i]:
            sensorid = data[i]['includeid']
            ydistance = [10,13,15,15,15,15,15,15]
            ax.annotate(sensorid, xy=(10, ydistance[n_subplots-1]),
                xycoords='axes points',
                horizontalalignment='left', verticalalignment='top')

        # ADD GRID:
        if grid:
            ax.grid(True,color=gridcolor,linewidth=0.5)

        # SET X-LABELS:
        timeunit = ''
	if confinex:
            tmin = np.min(t)
            tmax = np.max(t)
	    # --> If dates to be confined, set value types:
            _confinex(ax, tmax, tmin, timeunit)

        if i < n_subplots-1:
            setp(ax.get_xticklabels(), visible=False)
        else:
            ax.set_xlabel("Time (UTC) %s" % timeunit, color=labelcolor)

        # SET TICK TO ALTERNATING SIDES:
        if bool(i & 1):
            ax.yaxis.tick_right()
            ax.yaxis.set_label_position("right")

        # DEFINE MIN AND MAX ON Y-AXIS:
        ymin = data[i]['ymin']
        ymax = data[i]['ymax']
        ax.set_ylim(ymin,ymax)

        # APPLY FORMATTERS:
        label = data[i]['ylabel']
        ax.set_ylabel(label, color=labelcolor)
        ax.get_yaxis().set_major_formatter(plt_fmt)

    #----------------------------------------------------------------
    # PART 3: Finalising and saving plot
    #----------------------------------------------------------------

    # BUNDLE UP ALL SUBPLOTS:
    fig.subplots_adjust(hspace=0)

    # ADJUST X-AXIS FOR FULLDAY PLOTTING:
    if fullday:
        ax.set_xlim(np.floor(np.round(np.min(t)*100)/100),np.floor(np.max(t)+1))

    # SAVE OR SHOW:
    if outfile:
        path = os.path.split(outfile)[0]
        if not path == '': 
            if not os.path.exists(path):
                os.makedirs(path)
        if fmt: 
            fig.savefig(outfile, format=fmt, dpi=savedpi) 
        else: 
            print savedpi
            fig.savefig(outfile, dpi=savedpi) 
    else: 
        plt.show()


def _confinex(ax, tmax, tmin, timeunit):

    trange = tmax - tmin
    loggerstream.debug('plot: x range = %s' % str(trange))
    if trange < 0.0001: # 8 sec level --> set 0.5 second
        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%S'))
        timeunit = '[Sec]'
    elif trange < 0.01: # 13 minute level
        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%M:%S'))
        timeunit = '[M:S]'
    elif trange <= 1: # day level -->  set 1 hour
        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
        timeunit = '[H:M]'
    elif trange < 7: # 3 day level
        if trange < 2:
            ax.get_xaxis().set_major_locator(matplotlib.dates.HourLocator(interval=6))
        elif trange < 5:
            ax.get_xaxis().set_major_locator(matplotlib.dates.HourLocator(interval=12))
        else:
            ax.get_xaxis().set_major_locator(matplotlib.dates.WeekdayLocator(byweekday=matplotlib.dates.MO))
        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%d.%b\n%H:%M'))
        setp(ax.get_xticklabels(),rotation='0')
        timeunit = '[Day-H:M]'
    elif trange < 60: # month level
        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%d.%b'))
        setp(ax.get_xticklabels(),rotation='70')
        timeunit = '[Day]'
    elif trange < 150: # year level
        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%d.%b\n%Y'))
        setp(ax.get_xticklabels(),rotation='0')
        timeunit = '[Day]'
    elif trange < 600: # minute level
        if trange < 300:
            ax.get_xaxis().set_major_locator(matplotlib.dates.MonthLocator(interval=1))
        elif trange < 420:
            ax.get_xaxis().set_major_locator(matplotlib.dates.MonthLocator(interval=2))
        else:
            ax.get_xaxis().set_major_locator(matplotlib.dates.MonthLocator(interval=4))
        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%b %Y'))
        setp(ax.get_xticklabels(),rotation='0')
        timeunit = '[Month]'
    else:
        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%Y'))
        timeunit = '[Year]'

# TODO: This part of plot still needs to be added into new _plot.
'''

    def plot(self, keys, debugmode=None, **kwargs):


		# -- Shade in areas of storm phases:
                if plotphases:
                    if not stormphases:
                        loggerstream.warning('plot: Need phase definition times in "stormphases" list variable.')
		    if len(stormphases) < 4:
		        loggerstream.warning('plot: Incorrect number of phase definition times in variable shadephases. 4 required.')
                    else:
                        t_ssc = stormphases[0]
                        t_mphase = stormphases[1]
                        t_recphase = stormphases[2]
                        t_end = stormphases[3]

                    if key in plotphases:
                        try: 
                            ax.axvspan(t_ssc, t_mphase, facecolor='red', alpha=0.3, linewidth=0)
        		    ax.axvspan(t_mphase, t_recphase, facecolor='yellow', alpha=0.3, linewidth=0)
        		    ax.axvspan(t_recphase, t_end, facecolor='green', alpha=0.3, linewidth=0)
		        except:
                            loggerstream.error('plot: Error plotting shaded phase regions.')

		# -- Plot phase types with shaded regions:

                if not annoxy:
                    annoxy = {}
                if annophases:
                    if not stormphases:
                        loggerstream.debug('Plot: Need phase definition times in "stormphases" variable to plot phases.')
		    if len(stormphases) < 4:
                        loggerstream.debug('Plot: Incorrect number of phase definition times in variable shadephases. 4 required, %s given.' % len(stormphases))
                    else:
                        t_ssc = stormphases[0]
                        t_mphase = stormphases[1]
                        t_recphase = stormphases[2]
                        t_end = stormphases[3]

		    if key == plotphases[0]:
                        try: 
                            y_auto = [0.85, 0.75, 0.70, 0.6, 0.5, 0.5, 0.5] 
                            y_anno = ymin + y_auto[len(keys)-1]*(ymax-ymin)
                            tssc_anno, issc_anno = self._find_nearest(np.asarray(t), date2num(t_ssc))
                            yt_ssc = yplt[issc_anno]
		            if 'sscx' in annoxy:	# parameters for SSC annotation.
                                x_ssc = annoxy['sscx']
                            else:
                                x_ssc = t_ssc-timedelta(hours=2)
		            if 'sscy' in annoxy:
                                y_ssc = ymin + annoxy['sscy']*(ymax-ymin)
                            else:
                                y_ssc = y_anno
		            if 'mphx' in annoxy:	# parameters for main-phase annotation.
                                x_mph = annoxy['mphx']
                            else:
                                x_mph = t_mphase+timedelta(hours=1.5)
		            if 'mphy' in annoxy:
                                y_mph = ymin + annoxy['mphy']*(ymax-ymin)
                            else:
                                y_mph = y_anno
		            if 'recx' in annoxy:	# parameters for recovery-phase annotation.
                                x_rec = annoxy['recx']
                            else:
                                x_rec = t_recphase+timedelta(hours=1.5)
		            if 'recy' in annoxy:
                                y_rec = ymin + annoxy['recy']*(ymax-ymin)
                            else:
                                y_rec = y_anno

                            if not yt_ssc > 0.:
                                loggerstream.debug('plot: No data value at point of SSC.')
                            ax.annotate('SSC', xy=(t_ssc,yt_ssc), 
					xytext=(x_ssc,y_ssc),
					bbox=dict(boxstyle="round", fc="0.95", alpha=0.6),
					arrowprops=dict(arrowstyle="->",
					shrinkA=0, shrinkB=1,
					connectionstyle="angle,angleA=0,angleB=90,rad=10"))
                            ax.annotate('Main\nPhase', xy=(t_mphase,y_mph), xytext=(x_mph,y_mph),
					bbox=dict(boxstyle="round", fc="0.95", alpha=0.6))
                            ax.annotate('Recovery\nPhase', xy=(t_recphase,y_rec),xytext=(x_rec,y_rec),
					bbox=dict(boxstyle="round", fc="0.95", alpha=0.6))
                        except: 
                            loggerstream.error('Plot: Error annotating shaded phase regions.')
                  



'''
