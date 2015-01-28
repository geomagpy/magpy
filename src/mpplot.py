'''
Path:			magpy.mpplot
Part of package:	stream (plot)
Type:			Library of matplotlib plotting functions

PURPOSE:
	This script provides multiple functions for plotting a stream as well
        as analysing various properties of a stream.
        All plots are done with python's matplotlib package.

CONTAINS:
    (MAIN...)
	plot:		(Func) Will plot variables from a single stream.
	plotStreams:	(Func) Plots multiple variables from multiple streams.
    (EXTENDED...)
        plotEMD:	(Func) Plots Empirical Mode Decomposition from opt.emd
        plotPS: 	(Func) Plots the power spectrum of a given key.
        plotSpectrogram:(Func) Plots spectrogram of a given key.
	plotStereoplot:	(Func) Plots stereoplot of inc and dec values.
	obspySpectrogram:(Func) Spectrogram plotting function taken from ObsPy.
    (HELPER/INTERNAL FUNCTIONS...)
	_plot:		(Func) ... internal function to funnel plot information
			into a matplotlib plot object.
	_confinex:	(Func) ... utility function of _plot.
	maskNAN:	(Func) ... utility function of _plot.
	nan_helper:	(Func) ... utility function of _plot.
	denormalize:	(Func) ... utility function of _plot.

DEPENDENCIES:
        magpy.stream
        magpy.opt.emd
	matplotlib

CALLED BY:
	External data plotting and analysis scripts only.
'''

from stream import *
'''
try:
    import matplotlib
    if not os.isatty(sys.stdout.fileno()):   # checks if stdout is connected to a terminal (if not, cron is starting the job)
        print "No terminal connected - assuming cron job and using Agg for matplotlib"
        matplotlib.use('Agg') # For using cron
except:
    print "Prob with matplotlib"

try:
    version = matplotlib.__version__.replace('svn', '')
    try:
        version = map(int, version.replace("rc","").split("."))
        MATPLOTLIB_VERSION = version
    except:
        version = version.strip("rc")
        MATPLOTLIB_VERSION = version
    print "Loaded Matplotlib - Version %s" % str(MATPLOTLIB_VERSION)
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize 
    from matplotlib import mlab 
    from matplotlib.dates import date2num, num2date
    import matplotlib.cm as cm
    from pylab import *
    from datetime import datetime, timedelta
except ImportError as e:
    loggerplot.error("plot package: Matplotlib import error. If missing, please install to proceed.")
    loggerplot.error("Error string: %s" % e)
    raise Exception("CRITICAL IMPORT ERROR FOR PLOT PACKAGE: Matplotlib import error.")
'''

# TODO:
# - Move all plotting functions over from stream.
#	STILL TO FIX:
#	spectrogram()
#		TODO: ORIGINAL FUNCTION HAS ERRORS.
#		renamed to plotSpectrogram
#		changed variable title to plottitle
#	obspyspectrogram()
#		renamed to obspySpectrogram.
#	DONE:
#	plot() + plotStreams()
#	powerspectrum()
#		renamed to plotPS
#		changed variable title to plottitle
#	stereoplot()
#		renamed to plotStereoplot
#
# KNOWN BUGS:
# - None? :)

colorlist =  ['b','g','m','c','y','k','b','g','m','c','y','k']
symbollist = ['-','-','-','-','-','-','-','-','-','-','-','-']
gridcolor = '#316931'
labelcolor = '0.2'

def ploteasy(stream):
    keys = stream._get_key_headers()
    try:
        sensorid = stream.header['SensorID']
    except:
        sensorid = ''
    datadate = datetime.strftime(num2date(stream[0].time),'%Y-%m-%d')
    plottitle = "%s (%s)" % (sensorid,datadate)
    print "Plotting keys:", keys
    plot_new(stream, keys,
		confinex = True,
                plottitle = plottitle)

#####################################################################
#								    #
#	MAIN PLOTTING FUNCTIONS				    	    #
#	(for plotting geomagnetic data)				    #
#								    #
#####################################################################

def plot_new(stream,variables,specialdict={},errorbars=False,padding=0,noshow=False,
	annotate=False,stormphases=False,colorlist=colorlist,symbollist=symbollist,
	t_stormphases=None,includeid=False,function=None,plottype='discontinuous',
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
        - fill:		(list = []) List of keys for which the plot uses fill_between
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
	- stormphases:	(bool/list) If True, will plot shaded and annotated storm phases.
			NOTE: Also requires variable t_stormphases.
	- symbollist:	(list) List of symbols to plot with. Default= '-' for all.
	- t_stormphases:(dict) Dictionary (2 <= len(dict) <= 4) containing datetime objects.
			dict('ssc') = time of SSC
			dict('mphase') = time of start of main phase / end of SSC
			dict('rphase') = time of start of recovery phase / end of main phase
			dict('stormend') = end of recovery phase

    RETURNS:
        - plot: 	(Pyplot plot) Returns plot as plt.show or savedfile
			if outfile is specified.

    EXAMPLE:
        >>> 

    APPLICATION:
        
    '''

    # Check lists for variables have correct length:
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

    # The follow four variables can be given in two ways:
    #    bool: annotate = True --> all plots will be annotated
    #    list: annotate = [False, True, False] --> only second plot will be annotated
    # These corrections allow for simple variable definition during use.
    if type(errorbars) == list:
        errorbars = [errorbars]
    else:
        errorbars = errorbars

    if type(stormphases) == list:
        stormphases = [stormphases]
    else:
        stormphases = stormphases

    if type(annotate) == list:
        annotate = [annotate]
    else:
        annotate = annotate

    if type(padding) == list:
        padding = [padding]
    else:
        padding = padding

    plotStreams([stream], [ variables ], specialdict=[specialdict],noshow=noshow,
	errorbars=errorbars,padding=padding,annotate=annotate,stormphases=stormphases,
	colorlist=colorlist,symbollist=symbollist,t_stormphases=t_stormphases,
	includeid=includeid,function=function,plottype=plottype,**kwargs)


def plotStreams(streamlist,variables,padding=None,specialdict={},errorbars=None,
	colorlist=colorlist,symbollist=symbollist,annotate=None,stormphases=None,
	t_stormphases={},includeid=False,function=None,plottype='discontinuous',
	noshow=False,**kwargs):
    '''
    DEFINITION:
        This function plots multiple streams in one plot for easy comparison.

    PARAMETERS:
    Variables:
        - streamlist: 	(list) A list containing the streams to be plotted 
			in a list, e.g.:
			[ stream1, stream2, etc...]
			[ fge, pos1, env1 ]
	- variables:	(list(list)) List containing the variables to be plotted
			from each stream, e.g:
			[ ['x'], ['f'], ['t1', 't2'] ]
    Args:
	LISTED VARIABLES:
	(NOTE: All listed variables must correspond in size to the variable list.)
	- annotate:	(bool/list(bool)) If True, will annotate plot with flags, e.g.:
			[ [True], [True], [False, False] ]
			(Enter annotate = True for all plots to be annotated.)
	- errorbars:	(bool/list(bool)) If True, will plot corresponding errorbars:
			[ [False], [True], [False, False] ]
			(Enter errorbars = True to plot error bars on all plots.)
        - padding: 	(float/list(list)) List of lists containing paddings for each
			respective variable, e.g:
			[ [5], [5], [0.1, 0.2] ]
			(Enter padding = 5 for all plots to use 5 as padding.)
	- stormphases:	(bool/list(bool)) If True, will plot shaded and annotated storm phases.
			(Enter stormphases = True to plot storm on all plots.)
			NOTE: Also requires variable t_stormphases.
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
	- noshow:	(bool) If True, figure object will be returned. Default=False
	- outfile:	(str) Path of file to plot figure to.
	- plottitle:	(str) Title to put at top of plot.
	- plottype:	(NumPy str='discontinuous') Can also be 'continuous'.
	- savedpi:	(float=80) Determines dpi of outfile.
	- symbollist:	(list) List of symbols to plot with. Default= '-' for all.
	- t_stormphases:(dict) Dictionary (2 <= len(dict) <= 4) containing datetime objects.
			dict('ssc') = time of SSC
			dict('mphase') = time of start of main phase / end of SSC
			dict('rphase') = time of start of recovery phase / end of main phase
			dict('stormend') = end of recovery phase
			WARNING: If recovery phase is defined as past the end of
			the data to plot, it will be plotted in addition to the
			actual data.

    RETURNS:
        - plot: 	(Pyplot plot) Returns plot as plt.show or saved file
			if outfile is specified.

    EXAMPLE:
        >>> plotStreams(streamlist, variables, padding=5, outfile='plots.png')

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

	# TO PLOT FOUR DIFFERENT STREAMS WITH 7 VARIABLES TO A FILE:
        plotStreams(streamlist, variables, padding=padding,specialdict=specialdict,
		annotate=annotate,includeid=True,errorbars=errorbars,
		outfile='plots/all_magn_cut1.png',
		plottitle="WIC: All Magnetometers (%s)" % date)

	# TO PLOT DATA AND RETURN A FIGURE FOR FURTHER:
        plot = plotStreams(streamlist, variables, noshow=True)
        plot.title("New title.")
        plot.savefig("newfig.png")
    '''

    num_of_var = 0
    for item in variables:
        num_of_var += len(item)
    if num_of_var > 9:
        loggerplot.error("plotStreams: Can't plot more than 9 variables, sorry.")
	raise Exception("Can't plot more than 9 variables!")

    # Check lists for variables have correct length:
    if len(symbollist) < num_of_var:
        loggerplot.error("plotStreams: Length of symbol list does not match number of variables.")
	raise Exception("Length of symbol list does not match number of variables.")
    if len(colorlist) < num_of_var:
        loggerplot.error("plotStreams: Length of color list does not match number of variables.")
	raise Exception("Length of color list does not match number of variables.")

    plot_dict = []
    count = 0

    # Iterate through each variable, create dict for each:
    for i in range(len(streamlist)):
        stream = streamlist[i]
        t = np.asarray([row[0] for row in stream])
        for j in range(len(variables[i])):
            data_dict = {}
            key = variables[i][j]
            loggerplot.info("plotStreams: Determining plot properties for key %s." % key)
            if not key in KEYLIST[1:16]:
                loggerplot.error("plot: Column key (%s) not valid!" % key)
                raise Exception("Column key (%s) not valid!" % key)
            ind = KEYLIST.index(key)
            y = np.asarray([row[ind] for row in stream])

            if len(y) == 0:
                loggerplot.error("plotStreams: Cannot plot stream of zero length!")

	    # Fix if NaNs are present:
            if plottype == 'discontinuous':
                y = maskNAN(y)
            else: 
                nans, test = nan_helper(y)
                newt = [t[idx] for idx, el in enumerate(y) if not nans[idx]]
                t = newt
                y = [el for idx, el in enumerate(y) if not nans[idx]]

            data_dict['key'] = key
            data_dict['tdata'] = t
            data_dict['ydata'] = y
            data_dict['color'] = colorlist[count]
            data_dict['symbol'] = symbollist[count]

            # Define padding for each variable:
            if padding:
                if type(padding) == list:
                    ypadding = padding[i][j]
                else:
                    ypadding = padding
            else:
                ypadding = 0

            # If limits are specified, use these:
            if specialdict:
                if key in specialdict[i]:
                    specialparams = specialdict[i][key]
                    data_dict['ymin'] = specialparams[0] - ypadding
                    data_dict['ymax'] = specialparams[1] + ypadding
                else:
                    if not (np.min(y) == np.max(y)):
                        data_dict['ymin'] = np.min(y) - ypadding
                        data_dict['ymax'] = np.max(y) + ypadding
                    else:
                        loggerplot.warning('plot: Min and max of key %s are equal. Adjusting axes.' % key)
                        data_dict['ymin'] = np.min(y) - 0.05
                        data_dict['ymax'] = np.max(y) + 0.05
            else:
                if not (np.min(y) == np.max(y)):
                    data_dict['ymin'] = np.min(y) - ypadding
                    data_dict['ymax'] = np.max(y) + ypadding
                else:
                    loggerplot.warning('plot: Min and max of key %s are equal. Adjusting axes.' % key)
                    data_dict['ymin'] = np.min(y) - 0.5
                    data_dict['ymax'] = np.max(y) + 0.5

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

            # Create array for errorbars:
            if errorbars:
                if type(errorbars) == list:
                    if errorbars[i][j]:
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

            # Annotate flagged data points:
            if annotate:
	        if type(annotate) == list:
                    if annotate[i][j]:
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

            # Plot a function:
            if function:
                data_dict['function'] = function

            # Plot shaded storm phases:
            if stormphases:
                if not t_stormphases:
                    loggerplot.error("plotStreams: No variable t_stormphases for plotting phases.")
                    raise Exception("Require variable t_stormphases when stormphases=True!")
                if len(t_stormphases) not in [1,2,3,4]:
                    loggerplot.error("plotStreams: Length of variable t_stormphases incorrect.")
                    raise Exception("Something is wrong with length of variable t_stormphases!")
                if type(stormphases) == list:
                    if stormphases[i][j]:
                        data_dict['stormphases'] = t_stormphases
                else:
                    data_dict['stormphases'] = t_stormphases

            # Include sensor IDs:
            if includeid:
                try:
                    sensor_id = stream.header['SensorID']
                    data_dict['sensorid'] = sensor_id
                except:
                    loggerplot.warning("plotStreams: No sensor ID to put into plot!")

            plot_dict.append(data_dict)
            count += 1

    loggerplot.info("plotStreams: Starting plotting function...")
    if not noshow:
        _plot(plot_dict, **kwargs)
        loggerplot.info("plotStreams: Plotting completed.")
    else:
        fig = _plot(plot_dict, noshow=True, **kwargs)
        loggerplot.info("plotStreams: Plotting completed.")
        return fig


def plotNormStreams(streamlist, key, normalize=True, normalizet=False,
	normtime=None, bgcolor='white', colorlist=colorlist, noshow=False, 
	outfile=None, plottitle=None, grid=True, gridcolor=gridcolor,
	labels=None, legendposition='upper right',labelcolor=labelcolor,
	returndata=False):
    '''
    DEFINITION:
        Will plot normalised streams. Streams will be normalized to a general
	median or to the stream values at a specific point in time.
	Useful for directly comparing streams in different locations.

    PARAMETERS:
    Variables:
        - streamlist: 	(list) A list containing the streams to be plotted. 
			e.g.:
			[ stream1, stream2, etc...]
			[ lemi1, lemi2, lemi3 ]
	- key:		(str) Variable to be compared
			'f'
    Args:
	- bgcolor:	(color='white') Background colour of plot.
	- colorlist:	(list(colors)) List of colours to plot with.
			Default = ['b','g','m','c','y','k','b','g','m','c','y','k']
	- grid:		(bool=True) If True, will plot grid.
	- gridcolor:	(color='#316931') Colour of grid.
	#- labelcolor:	(color='0.2') Colour of labels.
	- labels:	(list) Insert labels and legend for each stream, e.g.:
			['WIC', 'WIK', 'OOP']
	- legendposition: (str) Position of legend. Default = "upper right"
	- outfile:	(str) Path of file to plot figure to.
	- normalize:	(bool) If True, variable will be normalized to 0. Default = True.
	- normalizet:	(bool) If True, time variable will be normalized to 0. Default = False
	- normtime:	(datetime object/str) If streams are to be normalized, normtime
			is the time to use as a reference.
	- noshow:	(bool) Will return figure object at end if True, otherwise only plots
	- plottitle:	(str) Title to put at top of plot.
	#- plottype:	(NumPy str='discontinuous') Can also be 'continuous'.
	- returndata:	(bool) If True, will return normalised data arrays. Default = False.
	#- savedpi:	(float=80) Determines dpi of outfile.

    RETURNS:
        - plot: 	(Pyplot plot) Returns plot as plt.show or saved file
			if outfile is specified.

    EXAMPLE:
	>>>
    '''

    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)

    arraylist = []

    if labels:
        if len(labels) != len(streamlist):
            loggerplot.warning("plotNormStreams: Number of labels does not match number of streams!")

    for i, stream in enumerate(streamlist):
        loggerplot.info("plotNormStreams: Adding stream %s of %s..." % ((i+1),len(streamlist)))
        y = stream._get_column(key)
        t = stream._get_column('time')
        xlabel = "Time (UTC)"
        color = colorlist[i]

        if len(y) == 0:
            loggerplot.error("plotNormStreams: stream with empty array!")
            return
        try:
            yunit = stream.header['unit-col-'+key]
        except:
            yunit = ''
        #ylabel = stream.header['col-'+key].upper()+' $['+re.sub('[#$%&~_^\{}]', '', yunit)+']$'
        ylabel = stream.header['col-'+key].upper()+' $['+re.sub('[#$%&~_\{}]', '', yunit)+']$'

        # NORMALIZE VARIABLE:
        if normalize:
            if normtime:
                if type(normtime) == list:
                    normtime_start, normtime_end = test_time(normtime[0]), test_time(normtime[1])
                    normarea = stream.trim(normtime_start,normtime_end,newway=True)
                    normvalue = normarea.mean(key,meanfunction='median')
                else:
                    normtime = test_time(normtime)
                    val, idx = find_nearest(t,date2num(normtime))
                    normvalue = y[idx]
            else:
                normvalue = np.median(y)
            y = y - normvalue
            ylabel = "normalized "+ylabel

        # NORMALIZE TIME:
        if normalizet:
            if normtime:
                zerotime = normtime
            else:
                zerotime = t[0]
            t = t - zerotime
            xlabel = "normalized "+xlabel

        if returndata:
            arraylist.append([t,y])

        # PLOT DATA:
        if labels:
            ax.plot(t,y,color+'-',label=labels[i])
        else:
            ax.plot(t,y,color+'-')

    # ADD GRID:
    if grid:
        ax.grid(True,color=gridcolor,linewidth=0.5)

    # SET LABELS:
    ax.set_xlabel(xlabel, color=labelcolor)
    ax.set_ylabel(ylabel, color=labelcolor)
    ax.set_title(plottitle)
    ax.set_xlim([t[0],t[-1]])

    # INSERT LEGEND:
    if labels:
        legend = ax.legend(loc=legendposition, shadow=True)
        for label in legend.get_texts():
            label.set_fontsize('small')

    # FINALISE PLOT:
    if noshow == True and returndata == True:
        return [fig, arraylist]
    elif noshow == False and returndata == True:
        return arraylist
    elif noshow == True and returndata == False:
        return fig
    else:
        if outfile:
            plt.savefig(outfile)
        else:
            plt.show()


#####################################################################
#								    #
#	EXTENDED PLOTTING FUNCTIONS				    #
#	(for more advanced functions)				    #
#								    #
#####################################################################


def plotEMD(stream,key,verbose=False,plottitle=None,
	outfile=None,sratio=0.25):
    '''
    DEFINITION:
	NOTE: EXPERIMENTAL FUNCTION ONLY.
        Function for plotting Empirical Mode Decomposition of
	DataStream. Currently only optional function.
	(Adapted from RL code in MagPyAnalysis/NoiseFloor_Spectral/magemd.py.)

    PARAMETERS:
    Variables:
        - stream: 	(DataStream object) Description.
        - key: 		(str) Key in stream to apply EMD to.
    Kwargs:
	- outfile:	(str) Save plot to file. If no file defined, plot
			will simply be shown.
	- plottitle:	(str) Title to place at top of plot.
	- sratio:	(float) Decomposition percentage. Determines how curve
			is split. Default = 0.25.
        - verbose: 	(bool) Print results. Default False.

    RETURNS:
        - plot: 	(matplotlib plot) Plot depicting the modes.

    EXAMPLE:
        >>> plotEMDAnalysis(stream,'x')

    APPLICATION:
    '''

    # TODO:
    # - make axes easier to read
    # - add a amplitude statistic (histogram)
    # - add a haeufigkeit plot perpendicular to the diagrams 
    import opt.emd as emd # XXX: add this into main program when method is finalised

    loggerplot.info("plotEMD: Starting EMD calculation.")

    col = stream._get_column(key)
    timecol = stream._get_column('time')
    if verbose:
        print "Amount of values and standard deviation:", len(col), col.std()
    res = emd.emd(col,max_modes=20)
    if verbose:
        print "Found the follwing amount of decomposed modes:", len(res)
    separate = int(np.round(len(res)*sratio,0))
    if verbose:
        print "Separating the last N curves as smooth. N =",separate
    stdarray = []
    newcurve = [0]*len(res[0])
    noisecurve = [0]*len(res[0])
    smoothcurve = [0]*len(res[0])
    f, axarr = plt.subplots(len(res), sharex=True)

    for i, elem in enumerate(res):
        axarr[i].plot(elem)
        newcurve = [x + y for x, y in zip(newcurve, elem)]
        stdarray.append([i,elem.std()])
        ds = stream
        ds._put_column(elem,'x')
        ds._put_column(timecol,'time')

        if i >= len(res)-separate:
            if verbose:
                print "Smooth:", i
            smoothcurve = [x + y for x, y in zip(smoothcurve, elem)]
        if i < len(res)-separate:
            if verbose:
                print "Noise:", i
            noisecurve = [x + y for x, y in zip(noisecurve, elem)]

    plt.show()

    plt.plot(smoothcurve)
    plt.plot(newcurve)
    plt.title("Variation of H component")
    plt.xlabel("Time [seconds of day]")
    plt.ylabel("F [nT]")
    plt.legend()
    plt.show()

    plt.plot(noisecurve)
    plt.title("Variation of H component - high frequency content")
    plt.xlabel("Time [seconds of day]")
    plt.ylabel("F [nT]")
    plt.show()

    plt.close()
    stdarray = np.asarray(stdarray)
    ind = stdarray[:,0]
    val = stdarray[:,1]
    plt.bar(ind,val)
    plt.title("Standard deviation of EMD modes")
    plt.xlabel("EMD mode")
    plt.ylabel("Standard deviation [nT]")
    plt.show()


def plotPS(stream,key,debugmode=False,outfile=None,noshow=False,
	returndata=False,freqlevel=None,marks={},fmt=None,
	axes=None,plottitle=None,**kwargs):
    """
    DEFINITION:
        Calculate the power spectrum following the numpy fft example
	and plot the results.

    PARAMETERS:
    Variables:
        - stream:	(DataStream object) Stream to analyse
        - key:		(str) Key to analyse
    Kwargs:
	- axes:		(?) ?
        - debugmode: 	(bool) Variable to show steps
	- fmt:		(str) Format of outfile, e.g. "png"
	- freqlevel:	(float) print noise level at that frequency.
	- marks:	(dict) Contains list of marks to add, e.g:
			{'here',1}
	- outfile:	(str) Filename to save plot to
	- plottitle:	(str) Title to display on plot
	- returndata:	(bool) Return frequency and asd 

    RETURNS:
        - plot: 	(matplotlib plot) A plot of the powerspectrum
	If returndata = True:
	- freqm:	(float) Maximum frequency
	- asdm:	(float) ?

    EXAMPLE:
        >>> plotPS(stream, 'x')
	OR
	>>> freq, a = plotPS(stream, 'x', returndata=True)

    APPLICATION:
        >>> import magpy
	1. Requires DataStream object:
        >>> data_path = '/usr/lib/python2.7/magpy/examples/*'
        >>> data = read(path_or_url=data_path,
			starttime='2013-06-10 00:00:00',
			endtime='2013-06-11 00:00:00')
	2. Call for data stream:
        >>> data.powerspectrum('f',
			plottitletitle='PSD of f', marks={'day':0.000011574},
			outfile='ps.png')
    """

    loggerplot.info("plotPS: Starting powerspectrum calculation.")
    
    if noshow:
        show = False
    else:
        show = True

    dt = stream.get_sampling_period()*24*3600

    if not len(stream) > 0:
        loggerplot.error("plotPS: Stream of zero length -- aborting.")
        raise Exception("Can't analyse power spectrum of stream of zero length!")

    t = np.asarray(stream._get_column('time'))
    val = np.asarray(stream._get_column(key))
    t_min = np.min(t)
    t_new, val_new = [],[]

    nfft = int(nearestPow2(len(t)))

    if nfft > len(t): 
        nfft = int(nearestPow2(len(t) / 2.0)) 

    for idx, elem in enumerate(val):
        if not isnan(elem):
            t_new.append((t[idx]-t_min)*24*3600)
            val_new.append(elem)

    t_new = np.asarray(t_new)
    val_new = np.asarray(val_new)

    if debugmode:
        print "Extracted data for powerspectrum at %s" % datetime.utcnow()

    if not axes: 
        fig = plt.figure() 
        ax = fig.add_subplot(111) 
    else: 
        ax = axes 

    psdm = mlab.psd(val_new, nfft, 1/dt) 
    asdm = np.sqrt(psdm[0]) 
    freqm = psdm[1] 

    ax.loglog(freqm, asdm,'b-')

    if debugmode:
        print "Maximum frequency:", max(freqm)

    if freqlevel:
        val, idx = find_nearest(freqm, freqlevel)
        if debugmode:
            print "Maximum Noise Level at %s Hz: %s" % (val,asdm[idx])

    if not marks:
        pass
    else:
        for elem in marks:
            ax.annotate(elem, xy=(marks[elem],min(asdm)), 
			xytext=(marks[elem],max(asdm)-(max(asdm)-min(asdm))*0.3),
			bbox=dict(boxstyle="round", fc="0.95", alpha=0.6),
			arrowprops=dict(arrowstyle="->",
			shrinkA=0, shrinkB=1,
			connectionstyle="angle,angleA=0,angleB=90,rad=10"))

    try:
        unit = stream.header['unit-col-'+key]
    except:
        unit = 'unit'

    ax.set_xlabel('Frequency $[Hz]$')
    ax.set_ylabel(('Amplitude spectral density $[%s/sqrt(Hz)]$') % unit) 
    if plottitle:
        ax.set_title(plottitle)

    loggerplot.info("Finished powerspectrum.")

    if outfile: 
        if fmt: 
            fig.savefig(outfile, format=fmt) 
        else: 
            fig.savefig(outfile) 
    elif returndata: 
        return freqm, asdm
    elif show: 
        plt.draw()	# show() should only ever be called once. Use draw() in between!
    else: 
        return fig 


def plotSpectrogram(stream, keys, per_lap=0.9, wlen=None, log=False, 
	outfile=None, fmt=None, axes=None, dbscale=False, 
	samp_rate_multiplicator=None,mult=8.0, cmap=None,zorder=None, 
	plottitle=None, show=True, sphinx=False, clip=[0.0, 1.0], **kwargs):
    """
    DEFINITION:
        Creates a spectrogram plot of selected keys.
        Parameter description at function obspyspectrogram

    PARAMETERS:
    Variables:
        - stream:	(DataStream object) Stream to analyse
        - keys:		(list) Keys to analyse
    Kwargs:
	- per_lap:	(?) ?
        - wlen: 	(?) ?
	- log:		(bool) ?
	- outfile:	(str) Filename to save plot to
	- fmt:		(str) Format of outfile, e.g. 'png'
	- axes:		(?) ?
	- dbscale:	(?) ?
	- mult:		(?) ?
	- cmap:		(?) ?
	- zorder:	(?) ?
	- plottitle:	(?) ?
	- samp_rate_multiplicator:
			(float=24*3600) Change the frequency relative to one day
			sampling rate given as days -> multiplied by x to create Hz, 
			Default 24, which means 1/3600 Hz
	- show:		(?) ?
	- sphinx:	(?) ?

    RETURNS:
        - plot: 	(matplotlib plot) A plot of the spectrogram.

    EXAMPLE:
        >>> plotSpectrogram(stream, ['x','y'])

    APPLICATION:
        >>>  
    """

    if not samp_rate_multiplicator:
        samp_rate_multiplicator = 24*3600

    t = stream._get_column('time')

    if not len(t) > 0:
        loggerplot.error('plotSpectrogram: stream of zero length -- aborting')
        return

    for key in keys:
        val = stream._get_column(key)
        val = maskNAN(val)
        dt = stream.get_sampling_period()*(samp_rate_multiplicator)
        Fs = float(1.0/dt)
        obspySpectrogram(val,Fs, per_lap=per_lap, wlen=wlen, log=log, 
                    outfile=outfile, fmt=fmt, axes=axes, dbscale=dbscale, 
                    mult=mult, cmap=cmap, zorder=zorder, title=plottitle, show=show, 
                    sphinx=sphinx, clip=clip)


def obspySpectrogram(data, samp_rate, per_lap=0.9, wlen=None, log=False, 
	outfile=None, fmt=None, axes=None, dbscale=False, 
	mult=8.0, cmap=None, zorder=None, title=None, show=True, 
	sphinx=False, clip=[0.0, 1.0]): 
    """
        Function taken from ObsPy
        Computes and plots spectrogram of the input data. 
        :param data: Input data 
        :type samp_rate: float 
        :param samp_rate: Samplerate in Hz 
        :type per_lap: float 
        :param per_lap: Percentage of overlap of sliding window, ranging from 0 
            to 1. High overlaps take a long time to compute. 
        :type wlen: int or float 
        :param wlen: Window length for fft in seconds. If this parameter is too 
            small, the calculation will take forever. 
        :type log: bool 
        :param log: Logarithmic frequency axis if True, linear frequency axis 
            otherwise. 
        :type outfile: String 
        :param outfile: String for the filename of output file, if None 
            interactive plotting is activated. 
        :type fmt: String 
        :param fmt: Format of image to save 
        :type axes: :class:`matplotlib.axes.Axes` 
        :param axes: Plot into given axes, this deactivates the fmt and 
            outfile option. 
        :type dbscale: bool 
        :param dbscale: If True 10 * log10 of color values is taken, if False the 
            sqrt is taken. 
        :type mult: float 
        :param mult: Pad zeros to lengh mult * wlen. This will make the spectrogram 
            smoother. Available for matplotlib > 0.99.0. 
        :type cmap: :class:`matplotlib.colors.Colormap` 
        :param cmap: Specify a custom colormap instance 
        :type zorder: float 
        :param zorder: Specify the zorder of the plot. Only of importance if other 
            plots in the same axes are executed. 
        :type title: String 
        :param title: Set the plot title 
        :type show: bool 
        :param show: Do not call `plt.show()` at end of routine. That way, further 
            modifications can be done to the figure before showing it. 
        :type sphinx: bool 
        :param sphinx: Internal flag used for API doc generation, default False 
        :type clip: [float, float] 
        :param clip: adjust colormap to clip at lower and/or upper end. The given 
            percentages of the amplitude range (linear or logarithmic depending 
            on option `dbscale`) are clipped. 
    """ 

    # enforce float for samp_rate
    samp_rate = float(samp_rate) 

    # set wlen from samp_rate if not specified otherwise 
    if not wlen: 
        wlen = samp_rate / 100. 

    npts = len(data) 

    # nfft needs to be an integer, otherwise a deprecation will be raised 
    #XXX add condition for too many windows => calculation takes for ever 
    nfft = int(nearestPow2(wlen * samp_rate))

    if nfft > npts:
        print npts
        nfft = int(nearestPow2(npts / 8.0)) 

    if mult != None: 
        mult = int(nearestPow2(mult)) 
        mult = mult * nfft 

    nlap = int(nfft * float(per_lap)) 

    data = data - data.mean() 
    end = npts / samp_rate 

    # Here we call not plt.specgram as this already produces a plot 
    # matplotlib.mlab.specgram should be faster as it computes only the 
    # arrays 
    # XXX mlab.specgram uses fft, would be better and faster use rfft 

    if MATPLOTLIB_VERSION >= [0, 99, 0]: 
        print "1", nfft, nlap
	# TODO: ERROR IS IN HERE
	#nfft = 256
	#nlap = 128
	# Default values don't help...
        specgram, freq, time = mlab.specgram(data, Fs=samp_rate, 
                                                    NFFT=nfft, noverlap=nlap) 
        print "2"
    else: 
        specgram, freq, time = mlab.specgram(data, Fs=samp_rate, 
                                                    NFFT=nfft, noverlap=nlap) 

    # db scale and remove zero/offset for amplitude 
    if dbscale: 
        specgram = 10 * np.log10(specgram[1:, :]) 
    else: 
        specgram = np.sqrt(specgram[1:, :]) 

    freq = freq[1:] 

    vmin, vmax = clip 

    if vmin < 0 or vmax > 1 or vmin >= vmax: 
        msg = "Invalid parameters for clip option." 
        raise ValueError(msg) 

    _range = float(specgram.max() - specgram.min()) 
    vmin = specgram.min() + vmin * _range 
    vmax = specgram.min() + vmax * _range 
    norm = Normalize(vmin, vmax, clip=True) 

    if not axes: 
        fig = plt.figure() 
        ax = fig.add_subplot(111) 
    else: 
        ax = axes 

    # calculate half bin width 
    halfbin_time = (time[1] - time[0]) / 2.0 
    halfbin_freq = (freq[1] - freq[0]) / 2.0 

    if log: 
        # pcolor expects one bin more at the right end 
        freq = np.concatenate((freq, [freq[-1] + 2 * halfbin_freq])) 
        time = np.concatenate((time, [time[-1] + 2 * halfbin_time])) 
        # center bin 
        time -= halfbin_time 
        freq -= halfbin_freq 
        # pcolormesh issue was fixed in matplotlib r5716 (2008-07-07) 
        # inbetween tags 0.98.2 and 0.98.3 
        # see:
        #  - http://matplotlib.svn.sourceforge.net/viewvc/... 
        #    matplotlib?revision=5716&view=revision 
        #  - http://matplotlib.sourceforge.net/_static/CHANGELOG 

        if MATPLOTLIB_VERSION >= [0, 98, 3]: 
            # Log scaling for frequency values (y-axis) 
            ax.set_yscale('log') 
            # Plot times 
            ax.pcolormesh(time, freq, specgram, cmap=cmap, zorder=zorder, 
                              norm=norm) 
        else: 
            X, Y = np.meshgrid(time, freq) 
            ax.pcolor(X, Y, specgram, cmap=cmap, zorder=zorder, norm=norm) 
            ax.semilogy() 
    else: 
        # this method is much much faster! 
        specgram = np.flipud(specgram) 
        # center bin 
        extent = (time[0] - halfbin_time, time[-1] + halfbin_time,
                  freq[0] - halfbin_freq, freq[-1] + halfbin_freq) 
        ax.imshow(specgram, interpolation="nearest", extent=extent, 
                  cmap=cmap, zorder=zorder) 

    # set correct way of axis, whitespace before and after with window 
    # length 
    ax.axis('tight') 
    ax.set_xlim(0, end) 
    ax.grid(False) 

    if axes: 
        return ax 

    ax.set_xlabel('Time [s]') 
    ax.set_ylabel('Frequency [Hz]') 
    if title: 
        ax.set_title(title)
            
    if not sphinx: 
        # ignoring all NumPy warnings during plot 
        temp = np.geterr() 
        np.seterr(all='ignore') 
        plt.draw() 
        np.seterr(**temp) 

    if outfile: 
        if fmt: 
            fig.savefig(outfile, format=fmt) 
        else: 
            fig.savefig(outfile) 
    elif show: 
        plt.show() 
    else: 
        return fig 


def plotStereoplot(stream,focus='all',colorlist = ['b','r','g','c','m','y','k'],
	bgcolor='#d5de9c',griddeccolor='#316931',gridinccolor='#316931',
	savedpi=80,legend=True,labellimit=11,legendposition="lower left",
	figure=False,noshow=False,plottitle=None,groups=None,outfile=None,**kwargs):
    """
    DEFINITION:
        Plots declination and inclination values in stereographic projection.
        Will abort if no idff typ is provided
        Full circles denote positive inclinations, open negative

    PARAMETERS:
    Variables:
	- stream	(DataStream) a magpy datastream object
    Kwargs:
	- bgcolor:	(colour='#d5de9c') Background colour
	- figure:	(bool) Show figure if True
	- focus:	(str) defines the plot area. Options:
                        all (default) - -90 to 90 deg inc, 360 deg dec
                        q1 - first quadrant
			q2 - first quadrant
			q3 - first quadrant
			q4 - first quadrant
			data - focus on data (if angular spread is less then 10 deg
	- gridcolor:	(str) Define grid color e.g. '0.5' greyscale, 'r' red, etc
	- griddeccolor: (colour='#316931') Grid colour for inclination
	- gridinccolor: (colour='#316931') Grid colour for declination
	- groups	(KEY) - key of keylist which defines color of points
                        (e.g. ('str2') in absolutes to select
                        different colors for different instruments 
	- legend:	(bool) - draws legend only if groups is given - default True
	- legendposition:
			(str) - draws the legend at chosen position,
			(e.g. "upper right", "lower center") - default is "lower left" 
	- labellimit:	(int)- maximum length of label in legend
	- noshow:	(bool) If True, will not call show at the end,
	- outfile:	(str) to save the figure, if path is not existing it will be created
	- savedpi:	(int) resolution
	- plottitle:	(str) Title at top of plot
                
    REQUIRES:
	- package operator for color selection

    RETURNS:
        - plot: 	(matplotlib plot) The stereoplot.

            ToDo:
                - add alpha 95 calc

    EXAMPLE:
	>>> stream.stereoplot(focus='data',groups='str2')

    APPLICATION:
	>>> 
    """

    loggerplot.info('plotStereoplot: Starting plot of stereoplot.')

    if not stream[0].typ == 'idff':
        loggerplot.error('plotStereoplot: idf data required for stereoplot.')
        raise Exception("Idf data required for plotting a stereoplot!")

    inc = stream._get_column('x')
    dec = stream._get_column('y')

    col = ['']
    if groups:
        sel = stream._get_column(groups)
        col = list(set(list(sel)))
        if len(col) > 7:
            col = col[:7]

    if not len(dec) == len(inc):
        loggerplot.error('plotStereoplot: Check input file - unequal inc and dec data?')
        return

    if not figure:
        fig = plt.figure()
    else:
        fig = figure
    ax = plt.gca()
    ax.cla() # clear things for fresh plot
    ax.set_aspect('equal')
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.set_xticks([])
    ax.set_yticks([])
    # Define coordinates:
    basic1=plt.Circle((0,0),90,color=bgcolor,fill=True)
    basic1a=plt.Circle((0,0),90,color=gridinccolor,fill=False)
    basic2=plt.Circle((0,0),30,color=gridinccolor,fill=False,linestyle='dotted')
    basic3=plt.Circle((0,0),60,color=gridinccolor,fill=False,linestyle='dotted')
    basic4=plt.Line2D([0,0],[-90,90],color=griddeccolor,linestyle='dashed')
    basic5=plt.Line2D([-90,90],[0,0],color=griddeccolor,linestyle='dashed')
    fig.gca().add_artist(basic1)
    fig.gca().add_artist(basic1a)
    fig.gca().add_artist(basic2)
    fig.gca().add_artist(basic3)
    fig.gca().add_artist(basic4)
    fig.gca().add_artist(basic5)
    for j in range(len(col)):
        color = colorlist[j]

        xpos,ypos,xneg,yneg,xabs,y = [],[],[],[],[],[]
        for i,el in enumerate(inc):
            if groups:
                if sel[i] == col[j]:
                    coinc = 90-np.abs(el)
                    sindec = np.sin(np.pi/180*dec[i])
                    cosdec = np.cos(np.pi/180*dec[i])
                    xabs.append(coinc*sindec)
                    y.append(coinc*cosdec)
                    if el < 0:
                        xneg.append(coinc*sindec)
                        yneg.append(coinc*cosdec)
                    else:
                        xpos.append(coinc*sindec)
                        ypos.append(coinc*cosdec)
            else:
                coinc = 90-np.abs(el)
                sindec = np.sin(np.pi/180*dec[i])
                cosdec = np.cos(np.pi/180*dec[i])
                xabs.append(coinc*sindec)
                y.append(coinc*cosdec)
                if el < 0:
                    xneg.append(coinc*sindec)
                    yneg.append(coinc*cosdec)
                else:
                    xpos.append(coinc*sindec)
                    ypos.append(coinc*cosdec)
                   
        xmax = np.ceil(max(xabs))
        xmin = np.floor(min(xabs))
        xdif = xmax-xmin 
        ymax = np.ceil(max(y))
        ymin = np.floor(min(y))
        ydif = ymax-ymin
        maxdif = max([xdif,ydif])
        mindec = np.floor(min(dec))
        maxdec = np.ceil(max(dec)) 
        mininc = np.floor(min(np.abs(inc)))
        maxinc = np.ceil(max(np.abs(inc))) 

        if focus == 'data' and maxdif <= 10:
            # decs
            startdec = mindec
            decline,inclst = [],[]
            startinc = mininc
            incline = []
            while startdec <= maxdec:
                xl = 90*np.sin(np.pi/180*startdec)
                yl = 90*np.cos(np.pi/180*startdec)
                decline.append([xl,yl,startdec])
                startdec = startdec+1
            while startinc <= maxinc:
                inclst.append(90-np.abs(startinc))
                startinc = startinc+1

        if focus == 'all':
            ax.set_xlim((-90,90))
            ax.set_ylim((-90,90))
        if focus == 'q1':
            ax.set_xlim((0,90))
            ax.set_ylim((0,90))
        if focus == 'q2':
            ax.set_xlim((-90,0))
            ax.set_ylim((0,90))
        if focus == 'q3':
            ax.set_xlim((-90,0))
            ax.set_ylim((-90,0))
        if focus == 'q4':
            ax.set_xlim((0,90))
            ax.set_ylim((-90,0))
        if focus == 'data':
            ax.set_xlim((xmin,xmax))
            ax.set_ylim((ymin,ymax))
            #ax.annotate('Test', xy=(1.2, 25.2))
        ax.plot(xpos,ypos,'o',color=color, label=col[j][:labellimit])
        ax.plot(xneg,yneg,'o',color='white')
        ax.annotate('60', xy=(0, 30))
        ax.annotate('30', xy=(0, 60))
        ax.annotate('0', xy=(0, 90))
        ax.annotate('90', xy=(90, 0))
        ax.annotate('180', xy=(0, -90))
        ax.annotate('270', xy=(-90, 0))

    if focus == 'data' and maxdif <= 10:
        for elem in decline:
            pline = plt.Line2D([0,elem[0]],[0,elem[1]],color=griddeccolor,linestyle='dotted')
            xa = elem[0]/elem[1]*((ymax - ymin)/2+ymin)
            ya = (ymax - ymin)/2 + ymin
            annotext = "D:%i" % int(elem[2]) 
            ax.annotate(annotext, xy=(xa,ya))
            fig.gca().add_artist(pline)
        for elem in inclst:
            pcirc = plt.Circle((0,0),elem,color=gridinccolor,fill=False,linestyle='dotted')
            xa = (xmax-xmin)/2 + xmin
            ya = sqrt((elem*elem)-(xa*xa))
            annotext = "I:%i" % int(90-elem) 
            ax.annotate(annotext, xy=(xa,ya))
            fig.gca().add_artist(pcirc)

    if groups and legend: 
        handles, labels = ax.get_legend_handles_labels()
        hl = sorted(zip(handles, labels),key=operator.itemgetter(1))
        handles2, labels2 = zip(*hl)
        ax.legend(handles2, labels2, loc=legendposition)

    if plottitle:
        ax.set_title(plottitle)
            
    # SAVE TO FILE (or show)
    if figure:
        return ax

    if outfile:
        path = os.path.split(outfile)[0]
        if not path == '': 
            if not os.path.exists(path):
                os.makedirs(path)
        if fmt: 
            fig.savefig(outfile, format=fmt, dpi=savedpi) 
        else: 
            fig.savefig(outfile, dpi=savedpi) 
    elif noshow:
        return fig
    else: 
        plt.show()


#####################################################################
#								    #
#	INTERNAL/HELPER FUNCTIONS				    #
#	(Best not play with these.)				    #
#								    #
#####################################################################


def _plot(data,savedpi=80,grid=True,gridcolor=gridcolor,noshow=False,
	bgcolor='white',plottitle=None,fullday=False,bartrange=0.06,
	labelcolor=labelcolor,confinex=False,outfile=None,stormanno_s=True,
	stormanno_m=True,stormanno_r=True,fmt=None,figure=False,fill=[]):
    '''
    For internal use only. Feed a list of dictionaries in here to plot.
    Every dictionary should contain all data needed for one single subplot.
    DICTIONARY STRUCTURE FOR EVERY SUBPLOT:
    [ { ***REQUIRED***
	'key'   : 'x'		(str) MagPy key
	'tdata' : t		(np.ndarray) Time
	'ydata' : y		(np.ndarray) Data y(t)
	'ymin'  : ymin		(float)	   Minimum of y-axis
	'ymax'  : ymax		(float)    Maximum of y-axis
	'symbol': '-'		(str)	   Symbol for plotting, '-' = line
	'color' : 'b'		(str)	   Colour of plotted line
	'ylabel': 'F [nt]'	(str)	   Label on y-axis
	'annotate': False	(bool)	   If this is True, must have 'flags' key
        'sensorid': 'LEMI025'	(str) 	   String pulled from header data. If available,
				will be plotted alongside data for clarity.

	OPTIONAL:
	'errorbars': eb		(np.ndarray) Errorbars to plot in subplot	
	'flags' : flags		(np.ndarray) Flags to add into subplot.
				Note: must be 2-dimensional, flags & comments.
	'function': fn		(function object) Plot a function within the subplot.
	} ,

      {	'key' : ...				} ... ]

    GENERAL VARIABLES:
    plottitle = "Data from 2014-05-02"
    confinex = False
    bgcolor = 'blue'
    etc. ... (all are listed in plot() and plotStreams() functions)
    figure  -- for GUI
    fill = ['x']
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
        key = data[i]['key']
        t = data[i]['tdata']
        y = data[i]['ydata']
        color = data[i]['color']
        symbol = data[i]['symbol']

        # CREATE SUBPLOT OBJECT & ADD TITLE:
        loggerplot.info("_plot: Adding subplot for key %s..." % data[i]['ylabel'])
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
				bartrange],[0,0,y[num]+0.1,y[num]+0.1],
				facecolor=cm.RdYlGn((9-y[num])/9.,1),alpha=1,edgecolor='k')
            ax.plot_date(t,y,color+'|')

        # --> Otherwise plot as normal:
        else:
            ax.plot_date(t,y,color+symbol)
            if key in fill:
                ax.fill_between(t,0,y,color=color)

        # DEFINE MIN AND MAX ON Y-AXIS:
        ymin = data[i]['ymin']
        ymax = data[i]['ymax']

        # PLOT ERROR BARS (if available):
        if 'errors' in data[i]:
            errorbars = data[i]['errors']
            ax.errorbar(t,y,yerr=errorbars,fmt=color+'o')

        # ANNOTATE:
        if data[i]['annotate'] == True:
            flags = data[i]['flags']
            emptycomment = "-"
            poslst = [ix for ix,el in enumerate(FLAGKEYLIST) if el == key]
            indexflag = int(poslst[0])
            for idx, elem in enumerate(flags[1]):
                if not elem == emptycomment and flags[0][idx][indexflag] in ['0','3']:
                    ax.annotate(r'%s' % (elem),
                                xy=(t[idx], y[idx]),
                                xycoords='data', xytext=(20, 20),
                                textcoords='offset points',
                                bbox=dict(boxstyle="round", fc="0.8"),
                                arrowprops=dict(arrowstyle="->",
                                shrinkA=0, shrinkB=1,
                                connectionstyle="angle,angleA=0,angleB=90,rad=10"))

	# PLOT A GIVEN FUNCTION:     
        if 'function' in data[i]:
            fkey = 'f'+key
            function = data[i]['function']
            if fkey in function[0]:
		# --> Get the minimum and maximum relative times
                ttmp = arange(0,1,0.0001)
                ax.plot_date(denormalize(ttmp,function[1],function[2]),function[0][fkey](ttmp),'r-')

        # PLOT SHADED AND ANNOTATED STORM PHASES:
        if 'stormphases' in data[i]:
            timespan = num2date(t[-1]) - num2date(t[0])
            y_pos = 0.9 # have at height 90% of total plot, x_pos(n)=1-(1-y_pos)*n
            y_anno = ymin + (1-(1-y_pos)*n_subplots)*(ymax-ymin)
            t_phases = data[i]['stormphases']
            if 'ssc' and 'mphase' in t_phases:
                t_ssc = t_phases['ssc']
                t_mphase = t_phases['mphase']
                ax.axvspan(t_ssc, t_mphase, facecolor='red', alpha=0.3, linewidth=0)
                if stormanno_s: # requirement so that only one plot is annotated
                    x_anno = t_ssc-timedelta(seconds=(timespan.seconds*0.1))
                    t_ssc_stream, idx_ssc = find_nearest(t, date2num(t_ssc))
                    y_ssc = y[idx_ssc]
                    ax.annotate('SSC', xy=(t_ssc,y_ssc), 
				xytext=(x_anno,y_anno),
				bbox=dict(boxstyle="round", fc="0.95", alpha=0.6),
				arrowprops=dict(arrowstyle="->",
				shrinkA=0, shrinkB=1,
				connectionstyle="angle,angleA=0,angleB=90,rad=10"))
                    stormanno_s = False
            if 'mphase' and 'rphase' in t_phases:
                t_mphase = t_phases['mphase']
                t_rphase = t_phases['rphase']
        	ax.axvspan(t_mphase, t_rphase, facecolor='yellow', alpha=0.3, linewidth=0)
                if stormanno_m:
                    x_anno = t_mphase+timedelta(seconds=(timespan.seconds*0.03))
                    t_mphase_stream, idx_mphase = find_nearest(t, date2num(t_mphase))
                    y_mphase = y[idx_mphase]
                    ax.annotate('Main\nPhase', xy=(t_mphase,y_mphase), 
				xytext=(x_anno,y_anno),
				bbox=dict(boxstyle="round", fc="0.95", alpha=0.6))
                    stormanno_m = False
            if 'rphase' and 'stormend' in t_phases:
                t_rphase = t_phases['rphase']
                t_stormend = t_phases['stormend']
        	ax.axvspan(t_rphase, t_stormend, facecolor='green', alpha=0.3, linewidth=0)
                if stormanno_r:
                    x_anno = t_rphase+timedelta(seconds=(timespan.seconds*0.03))
                    t_rphase_stream, idx_rphase = find_nearest(t, date2num(t_rphase))
                    y_rphase = y[idx_rphase]
                    ax.annotate('Recovery\nPhase', xy=(t_rphase,y_rphase),
				xytext=(x_anno,y_anno),
				bbox=dict(boxstyle="round", fc="0.95", alpha=0.6))
                    stormanno_r = False

	#------------------------------------------------------------
	# PART 2: Formatting the plot
	#------------------------------------------------------------

        # ADD SENSOR IDS TO DATA PLOTS:
	if 'sensorid' in data[i]:
            sensorid = data[i]['sensorid']
            ydistance = [13,13,15,15,15,15,15,15]
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

        # APPLY FORMATTERS:
        label = data[i]['ylabel']
        ax.set_ylim(ymin,ymax)
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
    # TODO the next two line are used for gui 
    #if figure:
    #    return ax
    if outfile:
        path = os.path.split(outfile)[0]
        if not path == '': 
            if not os.path.exists(path):
                os.makedirs(path)
        if fmt: 
            fig.savefig(outfile, format=fmt, dpi=savedpi) 
        else: 
            fig.savefig(outfile, dpi=savedpi) 
    elif noshow:
        return fig
    else: 
        plt.show()


def _confinex(ax, tmax, tmin, timeunit):
    """
    Automatically determines t-range so that the x-axis is easier
    on the eye.
    """

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

#####################################################################
#								    #
#	TESTING							    #
#	Run this after making changes:				    #
#	$ python mpplot.py					    #
#								    #
#####################################################################

if __name__ == '__main__':

    print
    print "----------------------------------------------------------"
    print "TESTING: PLOTTING PACKAGE"
    print "All plotting methods will be tested. This may take a while."
    print "A summary will be presented at the end. Any protocols"
    print "or functions with errors will be listed."
    print
    print "NOTE: This test requires graphical user interface"
    print "confirmation of package integrity for the majority of"
    print "functions. The plot titles specify what should be present"
    print "in the plot for it to have plotted successfully."
    print "    So get comfy and have a good look."
    print "----------------------------------------------------------"
    print
    
    print "Please enter path of a variometer data file for testing:"
    print "(e.g. /srv/archive/WIC/LEMI025/LEMI025_2014-05-07.bin)"
    while True:
        filepath = raw_input("> ")
        if os.path.exists(filepath):
            break
        else:
            print "Sorry, that file doesn't exist. Try again."
    print 
        
    now = datetime.utcnow()
    testrun = 'plottest_'+datetime.strftime(now,'%Y%m%d-%H%M')
    t_start_test = time.time()
    errors = {}

    print datetime.utcnow(), "- Starting plot package test. This run: %s." % testrun

    while True:

        # Step 1 - Read data
        try:
            teststream = read(filepath,tenHz=True)
            print datetime.utcnow(), "- Stream read in successfully."
        except Exception as excep:
            errors['read'] = str(excep)
            print datetime.utcnow(), "--- ERROR reading stream. Aborting test."
            break
        
        # Step 2 - Pick standard key for all other plots
        try:
            keys = teststream._get_key_headers()
            key = [keys[0]]
            key2 = [keys[0], keys[1]]
            print datetime.utcnow(), "- Using %s key for all subsequent plots." % key[0]
        except Exception as excep:
            errors['_get_key_headers'] = str(excep)
            print datetime.utcnow(), "--- ERROR getting default keys. Aborting test."
            break
        
        # Step 3 - Simple single plot with ploteasy
        try:
            ploteasy(teststream)
            print datetime.utcnow(), "- Plotted using ploteasy function."
        except Exception as excep:
            errors['ploteasy'] = str(excep)
            print datetime.utcnow(), "--- ERROR with ploteasy function. Aborting test."
            break
        
        # Step 4 - Standard plot
        try:
            plot_new(teststream,key,
			plottitle = "Simple plot of %s" % key[0])
            print datetime.utcnow(), "- Plotted standard plot."
        except Exception as excep:
            errors['plot-vanilla'] = str(excep)
            print datetime.utcnow(), "--- ERROR with standard plot. Aborting test."
            break
        
        # Step 5 - Multiple streams
        streamlist = 	[teststream, 	teststream	]
        variables =	[key,		key2		]
        try:
            plotStreams(streamlist, variables,
			plottitle = "Multiple streams: Three bars, top two should match.")
            print datetime.utcnow(), "- Plotted multiple streams."
        except Exception as excep:
            errors['plotStreams-vanilla'] = str(excep)
            print datetime.utcnow(), "--- ERROR with plotting multiple streams. Aborting test."
            break

        # Step 6 - Normalised stream comparison
        try:
            plotNormStreams([teststream], key[0],
			plottitle = "Normalized stream: Stream key should be normalized to zero.")
            print datetime.utcnow(), "- Plotted normalized streams."
        except Exception as excep:
            errors['plotNormStreams'] = str(excep)
            print datetime.utcnow(), "--- ERROR plotting normalized streams."

        # Step 7 - Flagged plot
        # ...

        # Step 8a - Plot with phases (single)
        t_start, t_end = teststream._find_t_limits()
        timespan = t_end - t_start
        t_stormphases = {}
        t_stormphases['ssc'] = t_start + timedelta(seconds=(timespan.seconds*0.2))
        t_stormphases['mphase'] = t_start + timedelta(seconds=(timespan.seconds*0.4))
        t_stormphases['rphase'] = t_start + timedelta(seconds=(timespan.seconds*0.6))
        t_stormphases['stormend'] = t_start + timedelta(seconds=(timespan.seconds*0.8))

        try:
            plot_new(teststream,key,
                        stormphases = True,
                        t_stormphases = t_stormphases,
			plottitle = "Single plot showing all THREE storm phases, annotated")
            print datetime.utcnow(), "- Plotted annotated single plot of storm phases."
        except Exception as excep:
            errors['plot-stormphases'] = str(excep)
            print datetime.utcnow(), "--- ERROR with storm phases plot."

        # Step 8b - Plot with phases (multiple)
        try:
            plotStreams(streamlist,variables,
                        stormphases = True,
                        t_stormphases = t_stormphases,
			plottitle = "Multiple plot showing all THREE storm phases, annotated")
            print datetime.utcnow(), "- Plotted annotated multiple plot of storm phases."
        except Exception as excep:
            errors['plotStreams-stormphases'] = str(excep)
            print datetime.utcnow(), "--- ERROR with storm phases multiple plot."
        
        # Step 9 - Plot power spectrum
        try:
            freqm, asdm = plotPS(teststream,key[0],
			returndata=True,
			marks={'Look here!':0.0001, '...and here!':0.01},
			plottitle = "Simple power spectrum plot with two marks")
            print datetime.utcnow(), "- Plotted power spectrum. Max frequency is at %s." % max(freqm)
        except Exception as excep:
            errors['plotPS'] = str(excep)
            print datetime.utcnow(), "--- ERROR plotting power spectrum."
        
        # Step 10 - Plot normal spectrogram
        try:
            plotSpectrogram(teststream,key2,
			plottitle = "Spectrogram of two keys")
            print datetime.utcnow(), "- Plotted spectrogram."
        except Exception as excep:
            errors['plotSpectrogram'] = str(excep)
            print datetime.utcnow(), "--- ERROR plotting spectrogram."
        
        # Step 11 - Plot function
        try:
            function = teststream.interpol(key,kind='quadratic')
            plot_new(teststream,key,function=function,
			plottitle = "Quadratic function plotted over original data.")
        except Exception as excep:
            errors['plot(function)'] = str(excep)
            print datetime.utcnow(), "--- ERROR plotting function."
        
        # Step 12 - Plot normal stereoplot
	# (This should stay as last step due to coordinate conversion.)
        try:
            teststream._convertstream('xyz2idf')
            plotStereoplot(teststream,
			plottitle="Standard stereoplot")
            print datetime.utcnow(), "- Plotted stereoplot."
        except Exception as excep:
            errors['plotStereoplot'] = str(excep)
            print datetime.utcnow(), "--- ERROR plotting stereoplot."
        
        # If end of routine is reached... break.
        break

    t_end_test = time.time()
    time_taken = t_end_test - t_start_test
    print datetime.utcnow(), "- Stream testing completed in %s s. Results below." % time_taken

    print
    print "----------------------------------------------------------"
    if errors == {}:
        print "0 errors! Great! :)"
    else:
        print len(errors), "errors were found in the following functions:"
        print str(errors.keys())
        print
        print "Would you like to print the exceptions thrown?"
        excep_answer = raw_input("(Y/n) > ")
        if excep_answer.lower() == 'y':
            i = 0
            for item in errors:
                print errors.keys()[i] + " error string:"
                print "    " + errors[errors.keys()[i]]
                i += 1
    print
    print "Good-bye!"
    print "----------------------------------------------------------"

