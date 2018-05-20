'''
Path:                   magpy.mpplot
Part of package:        stream (plot)
Type:                   Library of matplotlib plotting functions

PURPOSE:
        This script provides multiple functions for plotting a stream as well
        as analysing various properties of a stream.
        All plots are done with python's matplotlib package.

CONTAINS:
    (MAIN...)
        plot:           (Func) Will plot variables from a single stream.
        plotStreams:    (Func) Plots multiple variables from multiple streams.
        ploteasy:       (Func) Quick & easy plotting function that plots all data.
    (EXTENDED...)
        plotFlag:       (Func) Enables flagging in plot
        plotEMD:        (Func) Plots Empirical Mode Decomposition from magpy.opt.emd
        plotNormStreams:(Func) Plot normalised streams
        plotPS:         (Func) Plots the power spectrum of a given key.
        plotSatMag:     (Func) Useful tool for plotting magnetic and satellite data.
        plotSpectrogram:(Func) Plots spectrogram of a given key.
        plotStereoplot: (Func) Plots stereoplot of inc and dec values.
        obspySpectrogram:(Func) Spectrogram plotting function taken from ObsPy.
    (HELPER/INTERNAL FUNCTIONS...)
        _plot:          (Func) ... internal function to funnel plot information
                        into a matplotlib plot object.
        _confinex:      (Func) ... utility function of _plot.
        maskNAN:        (Func) ... utility function of _plot.
        nan_helper:     (Func) ... utility function of _plot.
        denormalize:    (Func) ... utility function of _plot.

DEPENDENCIES:
        magpy.stream
        magpy.opt.emd
        matplotlib

CALLED BY:
        External data plotting and analysis scripts only.
'''
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import division

from magpy.stream import *

import logging
logger = logging.getLogger(__name__)
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
    logger.error("plot package: Matplotlib import error. If missing, please install to proceed.")
    logger.error("Error string: %s" % e)
    raise Exception("CRITICAL IMPORT ERROR FOR PLOT PACKAGE: Matplotlib import error.")
'''

# TODO:
# - Move all plotting functions over from stream.
#       STILL TO FIX:
#       spectrogram()
#               TODO: ORIGINAL FUNCTION HAS ERRORS.
#               renamed to plotSpectrogram
#               changed variable title to plottitle
#       obspyspectrogram()
#               renamed to obspySpectrogram.
#       DONE:
#       plot() + plotStreams()
#       powerspectrum()
#               renamed to plotPS
#               changed variable title to plottitle
#       stereoplot()
#               renamed to plotStereoplot
#
# KNOWN BUGS:
# - None? :)

colorlist =  ['b','g','m','c','y','k','b','g','m','c','y','k']
symbollist = ['-','-','-','-','-','-','-','-','-','-','-','-']
gridcolor = '#316931'
labelcolor = '0.2'

try:
    defaultcolormap=plt.get_cmap('plasma')
except:
    defaultcolormap=cm.Accent


def ploteasy(stream):
    '''
    DEFINITION:
        Plots all data in stream. That's it.
        This function has no formatting options whatsoever.
        Very useful for quick & easy data evaluation.

    PARAMETERS:
    Variables:
        - stream:       (DataStream object) Stream to plot

    RETURNS:
        - plot:         (Pyplot plot) Returns plot as plt.show()

    EXAMPLE:
        >>> ploteasy(somedata)
    '''

    keys = stream._get_key_headers(numerical=True)
    if len(keys) > 9:
        keys = keys[:8]
    try:
        sensorid = stream.header['SensorID']
    except:
        sensorid = ''
    try:
        datadate = datetime.strftime(num2date(stream[0].time),'%Y-%m-%d')
    except:
        datadate = datetime.strftime(num2date(stream.ndarray[0][0]),'%Y-%m-%d')

    plottitle = "%s (%s)" % (sensorid,datadate)
    logger.info("Plotting keys:", keys)
    plot_new(stream, keys,
                confinex = True,
                plottitle = plottitle)

#####################################################################
#                                                                   #
#       MAIN PLOTTING FUNCTIONS                                     #
#       (for plotting geomagnetic data)                             #
#                                                                   #
#####################################################################

def plot_new(stream,variables=[],specialdict={},errorbars=False,padding=0,noshow=False,
        annotate=False,stormphases=False,colorlist=colorlist,symbollist=symbollist,
        t_stormphases=None,includeid=False,function=None,plottype='discontinuous',resolution=None,
        **kwargs):

        plot(stream,variables=variables,specialdict=specialdict,errorbars=errorbars,padding=padding,
                        noshow=noshow,annotate=annotate,stormphases=stormphases,colorlist=colorlist,
                        symbollist=symbollist,t_stormphases=t_stormphases,includeid=includeid,
                        function=function,plottype=plottype,resolution=resolution, **kwargs)


def plot(stream,variables=[],specialdict={},errorbars=False,padding=0,noshow=False,
        annotate=False,stormphases=False,colorlist=colorlist,symbollist=symbollist,
        t_stormphases=None,includeid=False,function=None,plottype='discontinuous',resolution=None,
        **kwargs):
    '''
    DEFINITION:
        This function creates a graph from a single stream.

    PARAMETERS:
    Variables:
        - stream:       (DataStream object) Stream to plot
        - variables:    (list) List of variables to plot.
    Kwargs:
        - annotate:     (bool/list=False) If True, will annotate plot.
        - bartrange:    (float) Variable for plotting of bars.
        - bgcolor:      (color='white') Background colour of plot.
        - colorlist:    (list(colors)) List of colours to plot with.
                        Default = ['b','g','m','c','y','k','b','g','m','c','y','k']
        - confinex:     (bool=False) x-axis will be confined to smaller t-values if True.
        - errorbars:    (bool/list=False) If True, will plot corresponding errorbars:
                        [ [False], [True], [False, False] ]
        - fill:         (list = []) List of keys for which the plot uses fill_between
        - fmt:          (str) Format of outfile.
        - fullday:      (bool=False) Will plot fullday if True.
        - function:     (func) [0] is a dictionary containing keys (e.g. fx),
                        [1] the startvalue, [2] the endvalue
                        Plot the content of function within the plot.
                        CAUTION: Not yet implemented.
        - grid:         (bool=True) If True, will plot grid.
        - gridcolor:    (color='#316931') Colour of grid.
        - includeid:    (bool) If True, sensor IDs will be extracted from header data and
                        plotted alongside corresponding data. Default=False
        - labelcolor:   (color='0.2') Colour of labels.
        - outfile:      (str) Path of file to plot figure to.
        - padding:      (float/list) Float or list of padding for each variable.
        - plottitle:    (str) Title to put at top of plot.
        - plottype:     (NumPy str='discontinuous') Can also be 'continuous'.
        - savedpi:      (float=80) Determines dpi of outfile.
        - specialdict:  (dictionary) contains special information for specific plots.
                        key corresponds to the column
                        input is a list with the following parameters
                        {'x':[ymin,ymax]}
        - stormphases:  (bool/list) If True, will plot shaded and annotated storm phases.
                        NOTE: Also requires variable t_stormphases.
        - symbollist:   (list) List of symbols to plot with. Default= '-' for all.
        - t_stormphases:(dict) Dictionary (2 <= len(dict) <= 4) containing datetime objects.
                        dict('ssc') = time of SSC
                        dict('mphase') = time of start of main phase / end of SSC
                        dict('rphase') = time of start of recovery phase / end of main phase
                        dict('stormend') = end of recovery phase

    RETURNS:
        - plot:         (Pyplot plot) Returns plot as plt.show or savedfile
                        if outfile is specified.

    EXAMPLE:
        >>>

    APPLICATION:

    '''

    # Test whether columns really contain data or whether only nans are present:
    stream = stream._remove_nancolumns()
    availablekeys = stream._get_key_headers(numerical=True)
    logger = logging.getLogger(__name__+".plot")

    # if no variables are given, use all available:
    if len(variables) < 1:
        variables = availablekeys
    else:
        variables = [var for var in variables if var in availablekeys]
    if len(variables) > 9:
        logger.info("More than 9 variables available - plotting only the first nine!")
        logger.info("Available: "+ str(variables))
        variables = variables[:9]
        logger.info("Plotting: "+ str(variables))
    else:
        logger.info("Plotting: "+ str(variables))

    # Check lists for variables have correct length:
    num_of_var = len(variables)
    if num_of_var > 9:
        logger.error("Can't plot more than 9 variables, sorry.")
        raise Exception("Can't plot more than 9 variables!")

    if len(symbollist) < num_of_var:
        logger.error("Length of symbol list does not match number of variables.")
        raise Exception("Length of symbol list does not match number of variables.")
    if len(colorlist) < num_of_var:
        logger.error("Length of color list does not match number of variables.")
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
        includeid=includeid,function=function,plottype=plottype,resolution=resolution,**kwargs)


def plotStreams(streamlist,variables,padding=None,specialdict={},errorbars=None,
        colorlist=colorlist,symbollist=symbollist,annotate=None,stormphases=None,
        t_stormphases={},includeid=False,function=None,plottype='discontinuous',
        noshow=False,labels=False,resolution=None,**kwargs):
    '''
    DEFINITION:
        This function plots multiple streams in one plot for easy comparison.

    PARAMETERS:
    Variables:
        - streamlist:   (list) A list containing the streams to be plotted
                        in a list, e.g.:
                        [ stream1, stream2, etc...]
                        [ fge, pos1, env1 ]
        - variables:    (list(list)) List containing the variables to be plotted
                        from each stream, e.g:
                        [ ['x'], ['f'], ['t1', 't2'] ]
    Args:
        LISTED VARIABLES:
        (NOTE: All listed variables must correspond in size to the variable list.)
        - annotate:     (bool/list(bool)) If True, will annotate plot with flags, e.g.:
                        [ [True], [True], [False, False] ]
                        (Enter annotate = True for all plots to be annotated.)
        - errorbars:    (bool/list(bool)) If True, will plot corresponding errorbars:
                        [ [False], [True], [False, False] ]
                        (Enter errorbars = True to plot error bars on all plots.)
        - labels:       [ (str) ] List of labels for each stream and variable, e.g.:
                        [ ['FGE'], ['POS-1'], ['ENV-T1', 'ENV-T2'] ]
        - padding:      (float/list(list)) List of lists containing paddings for each
                        respective variable, e.g:
                        [ [5], [5], [0.1, 0.2] ]
                        (Enter padding = 5 for all plots to use 5 as padding.)
        - stormphases:  (bool/list(bool)) If True, will plot shaded and annotated storm phases.
                        (Enter stormphases = True to plot storm on all plots.)
                        NOTE: Also requires variable t_stormphases.
        - specialdict:  (list(dict)) Same as plot variable, e.g:
                        [ {'z': [100,150]}, {}, {'t1':[7,8]} ]
        NORMAL VARIABLES:
        - bartrange:    (float) Variable for plotting of bars.
        - bgcolor:      (color='white') Background colour of plot.
        - colorlist:    (list(colors)) List of colours to plot with.
                        Default = ['b','g','m','c','y','k','b','g','m','c','y','k']
        - confinex:     (bool=False) x-axis will be confined to smaller t-values if True.
        - fmt:          (str) Format of outfile.
        - fullday:      (bool=False) Will plot fullday if True.
        - function:     (func) [0] is a dictionary containing keys (e.g. fx),
                        [1] the startvalue, [2] the endvalue
                        Plot the content of function within the plot.
                        CAUTION: Not yet implemented.
        - grid:         (bool=True) If True, will plot grid.
        - gridcolor:    (color='#316931') Colour of grid.
        - includeid:    (bool) If True, sensor IDs will be extracted from header data and
                        plotted alongside corresponding data. Default=False
        - labelcolor:   (color='0.2') Colour of labels.
        - opacity:      (0.0 to 1.0) Opacity applied to fills and bars.
        - legendposition: (str) Position of legend (when var labels is used), e.g. 'upper left'
        - noshow:       (bool) If True, figure object will be returned. Default=False
        - outfile:      (str) Path of file to plot figure to.
        - plottitle:    (str) Title to put at top of plot.
        - plottype:     (NumPy str='discontinuous') Can also be 'continuous'.
        - savedpi:      (float=80) Determines dpi of outfile.
        - symbollist:   (list) List of symbols to plot with. Default= '-' for all.
        - resolution:   (int) Resolution of plot. Amount of points are reduced to this value.
        - t_stormphases:(dict) Dictionary (2 <= len(dict) <= 4) containing datetime objects.
                        dict('ssc') = time of SSC
                        dict('mphase') = time of start of main phase / end of SSC
                        dict('rphase') = time of start of recovery phase / end of main phase
                        dict('stormend') = end of recovery phase
                        WARNING: If recovery phase is defined as past the end of
                        the data to plot, it will be plotted in addition to the
                        actual data.

    RETURNS:
        - plot:         (Pyplot plot) Returns plot as plt.show or saved file
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
        streamlist =    [ fge,                  cs,                     lemi025,        pos             ]
        variables =     [ ['x','y','z'],        ['f'],                  ['z'],          ['f']           ]
        specialdict =   [ {},                   {'f':[48413,48414]},    {},             {}              ]
        errorbars =     [ [False,False,False],  [False],                [False],        [True]          ]
        padding =       [ [1,1,1],              [1],                    [1] ,           [1]             ]
        annotate =      [ [False,False,False],  [True],                 [True] ,        [True]          ]

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
    
    logger = logging.getLogger(__name__+".plotStreams")

    # Preselect only numerical values
    variables = [[el for el in lst if el in NUMKEYLIST] for lst in variables]
    num_of_var = 0
    for item in variables:
        num_of_var += len(item)
    if num_of_var > 9:
        logger.error("Can't plot more than 9 variables, sorry.")
        raise Exception("Can't plot more than 9 variables!")

    # Check lists for variables have correct length:
    if len(symbollist) < num_of_var:
        logger.error("Length of symbol list does not match number of variables.")
        raise Exception("Length of symbol list does not match number of variables.")
    if len(colorlist) < num_of_var:
        logger.error("Length of color list does not match number of variables.")
        raise Exception("Length of color list does not match number of variables.")

    plot_dict = []
    count = 0

    if not resolution:
        resolution = 5000000  # 40 days of 1 second data can be maximaly shown in detail, 5 days of 10 Hz

    # Iterate through each variable, create dict for each:
    for i in range(len(streamlist)):
        stream = streamlist[i]
        ndtype = False
        try:
            t = stream.ndarray[KEYLIST.index('time')]
            lentime = len(t)
            if not lentime > 0:
                x=1/0
            if lentime > resolution:
                logger.info("Reducing data resultion ...")
                stepwidth = int(len(t)/resolution)
                t = t[::stepwidth]
                # Redetermine lentime
                lentime = len(t)
            logger.info("Start plotting of stream with length %i" % len(stream.ndarray[0]))
            ndtype = True
        except:
            t = np.asarray([row[0] for row in stream])
            logger.info("Start plotting of stream with length %i" % len(stream))
        #t = np.asarray([row[0] for row in stream])
        for j in range(len(variables[i])):
            data_dict = {}
            key = variables[i][j]
            logger.info("Determining plot properties for key %s." % key)
            if not key in NUMKEYLIST:
                logger.error("Column key (%s) not valid!" % key)
                raise Exception("Column key (%s) not valid!" % key)
            ind = KEYLIST.index(key)
            try:

                y = stream.ndarray[ind]
                if not len(y) > 0:
                    x=1/0
                if len(y) > resolution:
                    stepwidth = int(len(y)/resolution)
                    y = y[::stepwidth]
                if len(y) != lentime:
                    logger.error("Dimensions of time and %s do not match!" % key)
                    raise Exception("Dimensions of time and %s do not match!")
            except:
                y = np.asarray([float(row[ind]) for row in stream])
            #y = np.asarray([row[ind] for row in stream])

            if len(y) == 0:
                logger.error("Cannot plot stream of zero length!")

            # eventually remove flagged:
            dropflagged = False
            if dropflagged:
                flagind = KEYLIST.index('flag')
                flags = stream.ndarray[flagind]
                ind = KEYLIST.index(key)
                flagarray =  np.asarray([list(el)[ind] for el in flags])
                print("Flagarray", flagarray)
                indicies = np.where(flagarray == '1')
                print("Indicis", indicis)
                #for index in indicies:
                #    y[index] = NaN
                    #y[index] = float('nan')
                    #newflag = flags[0][ind]
                    #newflag[indexflag] = '0'
                    #data[i]['flags'][0][ind] == newflag
                #y = np.delete(np.asarray(y),indicies)


            #print len(t), len(y), np.asarray(y)

            # Fix if NaNs are present:
            if plottype == 'discontinuous':
                y = maskNAN(y)
            else:
                nans, test = nan_helper(y)
                newt = [t[idx] for idx, el in enumerate(y) if not nans[idx]]
                t = newt
                y = [el for idx, el in enumerate(y) if not nans[idx]]

            #print len(t), len(y), np.asarray(y), np.asarray(t)

            if len(y) == 0:
                logger.error("Cannot plot stream without data - Filling with 9999999!")
                if len(stream.ndarray[0]) > 0:
                    y = np.asarray([9999999 for row in stream.ndarray[0]])
                else:
                    y = np.asarray([9999999 for row in stream])


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
                ypadding = (np.max(y)- np.min(y))*0.05 # 0

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
                        logger.warning('Min and max of key %s are equal. Adjusting axes.' % key)
                        data_dict['ymin'] = np.min(y) - 0.05
                        data_dict['ymax'] = np.max(y) + 0.05
            else:
                if not (np.min(y) == np.max(y)):
                    data_dict['ymin'] = np.min(y) - ypadding
                    data_dict['ymax'] = np.max(y) + ypadding
                else:
                    logger.warning('Min and max of key %s are equal. Adjusting axes.' % key)
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
            elif yunit == None:
                logger.warning("No units for key %s! Empty column?" % key)
                label = ylabel
            else:
                label = ylabel
            data_dict['ylabel'] = label

            # Create array for errorbars:
            if errorbars:
                if type(errorbars) == list:
                    try:
                        if errorbars[i][j] and not key.startswith('d'):
                            ind = KEYLIST.index('d'+key)
                            if ndtype:
                                errors = stream.ndarray[ind]
                            else:
                                errors = np.asarray([row[ind] for row in stream])
                            if len(errors) > 0:
                                data_dict['errors'] = errors
                            else:
                                logger.warning("No errors for key %s. Leaving empty." % key)
                    except:
                        logger.warning("No errors for key %s. Leaving empty." % key)
                else:
                    try:
                        ind = KEYLIST.index('d'+key)
                        if ndtype:
                            errors = stream.ndarray[ind]
                        else:
                            errors = np.asarray([row[ind] for row in stream])
                        if len(errors) > 0:
                            data_dict['errors'] = errors
                        else:
                            logger.warning("No errors for key %s. Leaving empty." % key)
                    except:
                        logger.warning("No errors for key %s. Leaving empty." % key)

            # Annotate flagged data points:
            if annotate:
                if type(annotate) == list:
                    if annotate[i][j]:
                        if ndtype:
                            ind = KEYLIST.index('flag')
                            flag = stream.ndarray[ind]
                            ind = KEYLIST.index('comment')
                            comments = stream.ndarray[ind]
                        else:
                            flag = stream._get_column('flag')
                            comments = stream._get_column('comment')
                        flags = array([flag,comments], dtype=object)
                        data_dict['annotate'] = True
                        data_dict['flags'] = flags
                    else:
                        data_dict['annotate'] = False
                else:
                    if ndtype:
                        ind = KEYLIST.index('flag')
                        flag = stream.ndarray[ind]
                        ind = KEYLIST.index('comment')
                        comments = stream.ndarray[ind]
                    else:
                        flag = stream._get_column('flag')
                        comments = stream._get_column('comment')
                    flags = array([flag,comments], dtype=object)
                    #print "plotStreams1", flags
                    data_dict['annotate'] = True
                    data_dict['flags'] = flags
            else:
                data_dict['annotate'] = False

            #print "plotStreams2", data_dict['flags']

            # Get an existing function object from header:
            funclist = stream.header.get('DataFunction',[])
            if len(funclist) > 0:
                data_dict['function'] = funclist

            # Plot a function (overwrite any existing object):
            if function:
                data_dict['function'] = function

            # Plot shaded storm phases:
            if stormphases:
                if not t_stormphases:
                    logger.error("No variable t_stormphases for plotting phases.")
                    raise Exception("Require variable t_stormphases when stormphases=True!")
                if len(t_stormphases) not in [1,2,3,4]:
                    logger.error("Length of variable t_stormphases incorrect.")
                    raise Exception("Something is wrong with length of variable t_stormphases!")
                if type(stormphases) == list:
                    if stormphases[i][j]:
                        data_dict['stormphases'] = t_stormphases
                else:
                    data_dict['stormphases'] = t_stormphases

            # Add labels:
            if labels:
                data_dict['datalabel'] = labels[i][j]
            else:
                data_dict['datalabel'] = ''

            # Include sensor IDs:
            if includeid:
                try:
                    sensor_id = stream.header['SensorID']
                    data_dict['sensorid'] = sensor_id
                except:
                    logger.warning("No sensor ID to put into plot!")

            plot_dict.append(data_dict)
            count += 1

    logger.info("Starting plotting function...")
    if not noshow:
        _plot(plot_dict, **kwargs)
        logger.info("Plotting completed.")
    else:
        fig = _plot(plot_dict, noshow=True, **kwargs)
        logger.info("Plotting completed.")
        return fig


#####################################################################
#                                                                   #
#       EXTENDED PLOTTING FUNCTIONS                                 #
#       (for more advanced functions)                               #
#                                                                   #
#####################################################################



#####################################################################
#               Flagging                                            #
#####################################################################

def toggle_selector(event):
    print (' Key pressed.')
    if event.key in ['Q', 'q'] and toggle_selector.RS.active:
        print(' RectangleSelector deactivated.')
        toggle_selector.RS.set_active(False)
    if event.key in ['A', 'a'] and not toggle_selector.RS.active:
        print(' RectangleSelector activated.')
        toggle_selector.RS.set_active(True)

class figFlagger():

    def __init__(self, data = None, variables=None, figure=False):

        self.data = data

        self.offset = False
        self.mainnum = 1
        self.flagid = 3
        self.reason = 'why because'
        self.idxarray = []
        self.figure = False
        self.axes = False

        self.orgkeylist = self.data._get_key_headers()
        if not variables: #or variables == ['x','y','z','f'] or variables == ['x','y','z']:
            try:
                self.data = self.analyzeData(self.orgkeylist)
            except:
                logger.warning("figFlagger.__init__: You have to provide variables for this data set")


        keylist = self.data._get_key_headers(numerical=True)
        #print keylist
        if variables:
            keylist = variables
        if len(keylist) > 9:
            keylist = keylist[:8]
        #print keylist
        self.keylist = keylist
        annotatelist = [True if elem in self.orgkeylist else False for elem in keylist] # if elem in ['x','y','z'] else False]

        self.fig = plotStreams([self.data], [keylist], noshow=True, annotate=[annotatelist])

        radio, hzfunc = self.startup(self.fig, self.data)
        radio.on_clicked(hzfunc)

        if figure:
            self.figure = self.fig
            self.axes = self.fig.axes
        else:
            plt.show()

    def analyzeData(self,keylist):
        #keylist = self.data._get_key_headers()
        if not len(self.data.ndarray[0]) > 0:
            logger.warning("figFlagger.analyzeData: No ndarray found:")
            logger.warning("figFlagger.analyzeData: stream will be converted to ndarray type")
            self.data = self.data.linestruct2ndarray()

        if 'x' in keylist and 'y' in keylist and 'z' in keylist:
            self.data = self.data.differentiate(keys=['x','y','z'],put2keys = ['dx','dy','dz'])
            if 'f' in keylist:
                self.data = self.data.delta_f()
        return self.data

    def line_select_callback(self, eclick, erelease):
        'eclick and erelease are the press and release events'
        #print  "Selected line---:",self.mainnum
        x1, y1 = eclick.xdata, eclick.ydata
        x2, y2 = erelease.xdata, erelease.ydata
        #print "(%3.2f, %3.2f) --> (%3.2f, %3.2f)" % (x1, y1, x2, y2)
        #print " The button you used were: %s %s" % (eclick.button, erelease.button)
        self.selarray = [x1, y1, x2, y2]
        self.annotate(self.data, self.mainnum, self.selarray)

    def toggle_selector(self, event):
        #print (' Key pressed.')
        if event.key in ['Q', 'q'] and toggle_selector.RS.active:
            print(' RectangleSelector deactivated.')
            toggle_selector.RS.set_active(False)
        if event.key in ['A', 'a'] and not toggle_selector.RS.active:
            print(' RectangleSelector activated.')
            toggle_selector.RS.set_active(True)
        if event.key in ['F', 'f']:
            print(' Flag data:')
            print(' ------------------------------------------')
            print(" Selected data point:", len(self.idxarray))
            plt.clf()
            plt.close()
        if event.key in ['2']:
            print(' Setting default Flag ID to 2.')
            print(' ------------------------------------------')
            print(" -- keep data in any case - Observators decision")
            self.flagid = 2
        if event.key in ['3']:
            print(' Setting default Flag ID to 3.')
            print(' ------------------------------------------')
            print(" -- not to be used for definite - Observators decision")
            self.flagid = 3
        if event.key in ['L', 'l']:
            print(' Data:')
            print(' ------------------------------------------')
            print("Length:", data.length())
            #stream.write("")
        if event.key in ['O', 'o']:
            print(' Apply offset:')
            print(' ------------------------------------------')
            print(" Selected data point:", len(self.idxarray))
            self.offset = True
            plt.clf()
            plt.close()
        if event.key in ['H', 'h']:
            print(' Header:')
            print(' ------------------------------------------')
            print(data.header)
        if event.key in ['c', 'C']:
            print(' Close flagging and store data:')
            print(' ------------------------------------------')
            self.idxarray = 0
            plt.clf()
            plt.close('all')


    def annotate(self, data, numb, selarray):
        # Selecting the time range

        #print "Dataarray", data.ndarray

        selectedndarray = []
        keyar = []
        #print "Selected range:", selarray
        minbound = min([selarray[1],selarray[3]])
        maxbound = max([selarray[1],selarray[3]])
        idxstart = np.abs(data.ndarray[0].astype(float)-min(selarray[0],selarray[2])).argmin()
        idxend = np.abs(data.ndarray[0].astype(float)-max(selarray[0],selarray[2])).argmin()

        for i in range(len(data.ndarray)):
            if len(data.ndarray[i]) > idxstart: # and KEYLIST[i] in self.keylist:
                if KEYLIST[i] in self.keylist or KEYLIST[i] == 'time': #i in range(len(FLAGKEYLIST)) and
                    keyar.append(KEYLIST[i])
                    timear = data.ndarray[i][idxstart:idxend].astype(float)
                    selectedndarray.append(timear)
        selectedndarray = np.asarray(selectedndarray)

        newselectedndarray = []
        for i in range(len(selectedndarray)):
            allar = [elem for idx, elem in enumerate(selectedndarray[i]) if selectedndarray[numb][idx] >= minbound and selectedndarray[numb][idx] <= maxbound ]
            if i == 0:
                self.idxar = [idx+idxstart for idx, elem in enumerate(selectedndarray[i]) if selectedndarray[numb][idx] >= minbound and selectedndarray[numb][idx] <= maxbound ]
            newselectedndarray.append(allar)
        newselectedndarray = np.asarray(newselectedndarray).astype(float)
        self.idxar = np.asarray(self.idxar)
        # Some cleanup
        del selectedndarray

        self.markpoints(newselectedndarray,keyar)
        self.idxarray.extend(self.idxar)
        print("Selected %d points to annotate:" % len(self.idxarray))

    def markpoints(self, dataarray,keyarray):
        for idx,elem in enumerate(dataarray):
            key = keyarray[idx]
            #print "Selected curve - markpoints:", idx
            #print dataarray[idx]
            if not idx == 0 and not len(elem) == 0 and key in self.keylist: #FLAGKEYLIST:
                #print ( idx, self.axlist[idx-1] )
                ax = self.axlist[idx-1]
                #ax.clear()
                #ax.text(dataarray[0][1],dataarray[1][1], "(%s, %3.2f)"%("Hello",3.67), )
                ax.scatter(dataarray[0].astype('<f8'),elem.astype('<f8'), c='r', zorder=100) #, marker='d', c='r') #, zorder=100)

        #plt.connect('key_press_event', toggle_selector)
        plt.draw()

    def hzfunc(self,label):
        ax = self.hzdict[label]
        num = int(label.replace("plot ",""))
        #print "Selected axis number:", num
        #global mainnum
        self.mainnum = num
        # drawtype is 'box' or 'line' or 'none'
        toggle_selector.RS = RectangleSelector(ax, self.line_select_callback,
                                           drawtype='box', useblit=True,
                                           button=[1,3], # don't use middle button
                                           minspanx=5, minspany=5,
                                           spancoords='pixels',
                                           rectprops = dict(facecolor='red', edgecolor = 'black', alpha=0.2, fill=True))

        #plt.connect('key_press_event', toggle_selector)
        plt.draw()

    def flag(self, idxarray , flagid, reason, keylist):

        print("Flagging components %s with flagid %d, because of %s" % (','.join(keylist), flagid, reason))
        self.data = self.data.flagfast(idxarray, flagid, reason, keylist)


    def startup(self, fig, data):
        print("--------------------------------------------")
        print(" you started the build-in flagging function")
        print("--------------------------------------------")
        print("    -- use mouse to select rectangular areas")
        print("    -- press f for flagging this region")
        print("    --      press 2,3 to change default flag ID")
        print("    -- press l to get some basic data info")
        print("    -- press o to apply an offset")
        print("    -- press h to get all meta information")
        print("    -- press c to close the window and allow saving")

        # Arrays to exchange data
        self.selarray = []
        # Globals
        self.idxar = [] # holds all selected index values
        #mainnum = 1 # holds the selected figure axis

        self.axlist = fig.axes

        # #############################################################
        ## Adding Radiobttons to switch selector between different plot
        # #############################################################

        plt.subplots_adjust(left=0.2)
        axcolor = 'lightgoldenrodyellow'
        rax = plt.axes([0.02, 0.8, 0.10, 0.15])
        rax.patch.set_facecolor(axcolor)

        # create dict and list
        numlst = ['plot '+str(idx+1) for idx,elem in enumerate(self.axlist)]
        ## python 2.7 and higher
        #  self.hzdict = {'plot '+str(idx+1):elem for idx,elem in enumerate(self.axlist)}
        ## python 2.6 and lower
        self.hzdict = dict(('plot '+str(idx+1),elem) for idx,elem in enumerate(self.axlist))
        radio = RadioButtons(rax, numlst)

        # #############################################################
        ## Getting a rectangular selector
        # #############################################################

        toggle_selector.RS = RectangleSelector(self.axlist[0], self.line_select_callback, drawtype='box', useblit=True,button=[1,3],minspanx=5, minspany=5,spancoords='pixels', rectprops = dict(facecolor='red', edgecolor = 'black', alpha=0.2, fill=True))

        plt.connect('key_press_event', self.toggle_selector)

        return radio, self.hzfunc

def addFlag(data, flagger, indeciestobeflagged, variables):
        # INPUT section
        print("Provide flag ID (2 or 3):")
        print("  -- 2: keep data")
        print("  -- 3: remove data")
        flagid = raw_input(" -- default: %d \n" % flagger.flagid)
        print(flagid)
        reason = raw_input("Provide reason: \n")
        print(reason)
        flagkeys = raw_input("Keys to flag: e.g. x,y,z\n")
        if not flagkeys == '':
            if ',' in flagkeys:
                keylist = flagkeys.split(',')
                keylist = [elem for elem in keylist if elem in KEYLIST]
            else:
                if flagkeys in KEYLIST:
                    keylist = [flagkeys]
                else:
                    keylist = []
        else:
            keylist = []

        # ANALYSIS section
        try:
            flagid = int(flagid)
        except:
            flagid = flagger.flagid
        if not flagid in [0,1,2,3,4]:
            flagid = flagger.flagid
        if reason == '':
            reason = flagger.reason
        if keylist == []:
            keylist = [key for key in flagger.orgkeylist if key in FLAGKEYLIST]

        try:
            sensid = data.header["SensorID"]
        except:
            print("plotFlag: Flagging requires SensorID - set with stream.header['SensorID'] = 'MyID'")
            sensid = "Dummy_1234_0001"

        flaglst = []
        for k, g in groupby(enumerate(indeciestobeflagged), lambda ix: ix[0] - ix[1]):
            consecutives = map(itemgetter(1), g)
            #print consecutives
            begintime = num2date(data.ndarray[0][consecutives[0]].astype(float)).replace(tzinfo=None)
            endtime = num2date(data.ndarray[0][consecutives[-1]].astype(float)).replace(tzinfo=None)
            modtime = datetime.utcnow()
            #if option o:
            #datastream = datastream._select_timerange(begintime,endtime)
            #datastream = datastream.offset({key:value})
            #orgstream = orgstream.extend(datastream)
            # or  orgstream = datastream.extend(orgstream)
            #orgstream = orgstream.removeduplicates() ##if only last occurence is used
            for key in keylist:
                #print begintime, endtime, key, flagid, reason, sensid, modtime
                if not sensid == '':
                    flaglst.append([begintime, endtime, key, flagid, reason, sensid, modtime])

        # now flag the data and restart figure
        flagger.flag(indeciestobeflagged, flagid, reason, keylist)

        # reduce to original keys
        orgkeys = flagger.orgkeylist
        data = data.selectkeys(orgkeys)
        flagger = figFlagger(data, variables)
        #flagger.flag(data)
        return flagger.idxarray, flaglst

def plotFlag(data,variables=None,figure=False):
    '''
    DEFINITION:
        Creates a plot for flagging.
        Rectangular selection is possible and flagging can be conducted.
        Several additional keys provide data info.
    RETURNS:
        - stream:       (Datastream) ndarray stream to be saved
      optional
        - variables:    (list of keys)

    REQUIRES:
        - class figFlagger
    EXAMPLE:
        >>> flaggedstream = plotFlag(stream)
    '''
    flaglist = []
    flagdata = data.copy()

    flagger = figFlagger(flagdata,variables,figure)
    indeciestobeflagged = flagger.idxarray
    while indeciestobeflagged > 0:
        indeciestobeflagged, flaglst = addFlag(flagger.data, flagger, indeciestobeflagged, variables)
        flaglist.extend(flaglst)

    print("Returning data ....")
    try:
        print("  -- original format: %s " % data.header['DataFormat'])
    except:
        pass

    orgkeys = flagger.orgkeylist
    flagdata = flagger.data.selectkeys(orgkeys)
    return flagdata, flaglist

#####################################################################
#         End   Flagging                                            #
#####################################################################


def plotEMD(stream,key,verbose=False,plottitle=None,
        outfile=None,sratio=0.25,max_modes=20,hht=True,stackvals=[2,8,14]):
    '''
    DEFINITION:
        NOTE: EXPERIMENTAL FUNCTION ONLY.
        Function for plotting Empirical Mode Decomposition of
        DataStream. Currently only optional function.
        (Adapted from RL code in MagPyAnalysis/NoiseFloor_Spectral/magemd.py.)

    PARAMETERS:
    Variables:
        - stream:       (DataStream object) Description.
        - key:          (str) Key in stream to apply EMD to.
    Kwargs:
        - outfile:      (str) Save plot to file. If no file defined, plot
                        will simply be shown.
        - plottitle:    (str) Title to place at top of plot.
        - sratio:       (float) Decomposition percentage. Determines how curve
                        is split. Default = 0.25.
        - stackvals:    (list) provide a list of three intergers defining which components 
                        are to be stacked together. e.g. [2,8,14] means: 2 to 7 define the 
                        high frequ part, 8 to 14 the mid frequ, >14 the low frequ 
        - verbose:      (bool) Print results. Default False.

    RETURNS:
        - plot:         (matplotlib plot) Plot depicting the modes.

    EXAMPLE:
        >>> plotEMDAnalysis(stream,'x')

    APPLICATION:
    '''

    # TODO:
    # - make axes easier to read
    # - add a amplitude statistic (histogram)
    # - add a haeufigkeit plot perpendicular to the diagrams
    import magpy.opt.emd as emd # XXX: add this into main program when method is finalised

    logger.info("plotEMD: Starting EMD calculation.")

    col = stream._get_column(key)
    timecol = stream._get_column('time')
    logger.debug("plotEMD: Amount of values and standard deviation:", len(col), np.std(col))

    res = emd.emd(col,max_modes=max_modes)
    logger.debug("plotEMD: Found the following amount of decomposed modes:", len(res))
    separate = int(np.round(len(res)*sratio,0))
    logger.debug("plotEMD: Separating the last N curves as smooth. N =",separate)
    stdarray = []
    newcurve = [0]*len(res[0])
    noisecurve = [0]*len(res[0])
    midcurve = [0]*len(res[0])
    smoothcurve = [0]*len(res[0])
    f, axarr = plt.subplots(len(res), sharex=True)

    for i, elem in enumerate(res):
        axarr[i].plot(elem)
        newcurve = [x + y for x, y in zip(newcurve, elem)]
        stdarray.append([i,np.std(elem)])
        ds = stream
        ds._put_column(elem,'x')
        ds._put_column(timecol,'time')

        """
        if i >= len(res)-separate:
            if verbose:
                print "Smooth:", i
            smoothcurve = [x + y for x, y in zip(smoothcurve, elem)]
        if i < len(res)-separate:
            if verbose:
                print "Noise:", i
            noisecurve = [x + y for x, y in zip(noisecurve, elem)]
        """
        if i > stackvals[2]: # 14 - add stackvals = [2,8,14]
            logger.debug("plotEMD: Smooth: {}".format(i))
            smoothcurve = [x + y for x, y in zip(smoothcurve, elem)]
        if stackvals[1] <= i <= stackvals[2]:
            logger.debug("plotEMD: Mid: {}".format(i))
            midcurve = [x + y for x, y in zip(midcurve, elem)]
        if stackvals[0] < i < stackvals[1]:
            logger.debug("plotEMD: Noise: {}".format(i))
            noisecurve = [x + y for x, y in zip(noisecurve, elem)]

    plt.show()

    plt.plot(smoothcurve)
    #plt.plot(newcurve)
    plt.title("Variation of IMF 14 to 17 component - low frequency content")
    plt.xlabel("Time [15 min counts]")
    plt.ylabel("Counts/min")
    plt.legend()
    plt.show()

    plt.plot(noisecurve)
    plt.title("Variation of IMF 1 to 8 component - high frequency content")
    plt.xlabel("Time [15 min counts]")
    plt.ylabel("Counts/min")
    plt.show()

    plt.plot(midcurve)
    plt.title("Variation of IMF 9 to 12 - mid frequency content")
    plt.xlabel("Time [15 min counts]")
    plt.ylabel("Counts/min")
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

    if hht:
        print(emd.calc_inst_info(res,stream.samplingrate()))


def plotNormStreams(streamlist, key, normalize=True, normalizet=False,
        normtime=None, bgcolor='white', colorlist=colorlist, noshow=False,
        outfile=None, plottitle=None, grid=True, gridcolor=gridcolor,
        labels=None, legendposition='upper right',labelcolor=labelcolor,
        returndata=False,confinex=False,savedpi=80):
    '''
    DEFINITION:
        Will plot normalised streams. Streams will be normalized to a general
        median or to the stream values at a specific point in time.
        Useful for directly comparing streams in different locations.

    PARAMETERS:
    Variables:
        - streamlist:   (list) A list containing the streams to be plotted.
                        e.g.:
                        [ stream1, stream2, etc...]
                        [ lemi1, lemi2, lemi3 ]
        - key:          (str) Variable to be compared
                        'f'
    Args:
        - bgcolor:      (color='white') Background colour of plot.
        - colorlist:    (list(colors)) List of colours to plot with.
                        Default = ['b','g','m','c','y','k','b','g','m','c','y','k']
        - grid:         (bool=True) If True, will plot grid.
        - gridcolor:    (color='#316931') Colour of grid.
        #- labelcolor:  (color='0.2') Colour of labels.
        - labels:       (list) Insert labels and legend for each stream, e.g.:
                        ['WIC', 'WIK', 'OOP']
        - legendposition: (str) Position of legend. Default = "upper right"
        - outfile:      (str) Path of file to plot figure to.
        - normalize:    (bool) If True, variable will be normalized to 0. Default = True.
        - normalizet:   (bool) If True, time variable will be normalized to 0. Default = False
        - normtime:     (datetime object/str) If streams are to be normalized, normtime
                        is the time to use as a reference.
        - noshow:       (bool) Will return figure object at end if True, otherwise only plots
        - plottitle:    (str) Title to put at top of plot.
        #- plottype:    (NumPy str='discontinuous') Can also be 'continuous'.
        - returndata:   (bool) If True, will return normalised data arrays. Default = False.
        #- savedpi:     (float=80) Determines dpi of outfile.

    RETURNS:
        - plot:         (Pyplot plot) Returns plot as plt.show or saved file
                        if outfile is specified.

    EXAMPLE:
        >>>
    '''

    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)

    arraylist = []

    if labels:
        if len(labels) != len(streamlist):
            logger.warning("plotNormStreams: Number of labels does not match number of streams!")

    for i, stream in enumerate(streamlist):
        logger.info("plotNormStreams: Adding stream %s of %s..." % ((i+1),len(streamlist)))
        y = stream._get_column(key)
        t = stream._get_column('time')
        xlabel = "Time (UTC)"
        color = colorlist[i]

        if len(y) == 0:
            logger.error("plotNormStreams: stream with empty array!")
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

        # CONFINE X
        timeunit = ''
        if confinex:
            tmin = np.min(t)
            tmax = np.max(t)
            # --> If dates to be confined, set value types:
            _confinex(ax, tmax, tmin, timeunit)
        ax.set_xlabel("Time (UTC) %s" % timeunit, color=labelcolor)

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
            plt.savefig(outfile,dpi=savedpi)
        else:
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
        - stream:       (DataStream object) Stream to analyse
        - key:          (str) Key to analyse
    Kwargs:
        - axes:         (?) ?
        - debugmode:    (bool) Variable to show steps
        - fmt:          (str) Format of outfile, e.g. "png"
        - freqlevel:    (float) print noise level at that frequency.
        - marks:        (dict) Contains list of marks to add, e.g:
                        {'here',1}
        - outfile:      (str) Filename to save plot to
        - plottitle:    (str) Title to display on plot
        - returndata:   (bool) Return frequency and asd

    RETURNS:
        - plot:         (matplotlib plot) A plot of the powerspectrum
        If returndata = True:
        - freqm:        (float) Maximum frequency
        - asdm: (float) ?

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

    logger.info("plotPS: Starting power spectrum calculation")

    if noshow == True:
        show = False
    elif noshow == False:
        show = True
    else:
        logger.error("plotPS: Incorrect value ({:s}) for variable noshow.".format(noshow))
        raise ValueError("plotPS: Incorrect value ({:s}) for variable noshow.".format(noshow))

    dt = stream.get_sampling_period()*24*3600

    if not stream.length()[0] > 0:
        logger.error("plotPS: Stream of zero length -- aborting.")
        raise Exception("plotPS: Can't analyse power spectrum of stream of zero length!")

    t_new, val_new, nfft = _extract_data_for_PSD(stream, key)

    logger.debug("plotPS: Extracted data for powerspectrum at %s" % datetime.utcnow())

    if not axes:
        fig = plt.figure()
        ax = fig.add_subplot(111)
    else:
        ax = axes

    psdm = mlab.psd(val_new, nfft, 1/dt)
    asdm = np.sqrt(psdm[0])
    freqm = psdm[1]

    ax.loglog(freqm, asdm,'b-')

    logger.debug("Maximum frequency: {}".format(max(freqm)))

    if freqlevel:
        val, idx = find_nearest(freqm, freqlevel)
        logger.debug("Maximum Noise Level at %s Hz: %s" % (val,asdm[idx]))

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

    logger.info("Finished powerspectrum.")

    if outfile:
        if fmt:
            fig.savefig(outfile, format=fmt)
        else:
            fig.savefig(outfile)
    elif returndata:
        plt.close()
        return freqm, asdm
    elif show:
        plt.show()      # show() should only ever be called once. Use draw() in between!
    else:
        return fig
    
    plt.close()


def plotSatMag(mag_stream,sat_stream,keys,outfile=None,plottype='discontinuous',
        padding=5,plotfunc=True,confinex=True,labelcolor=labelcolor,savedpi=80,
        plotcolors=['#000066', '#C0C0C0'], plottitle=None,legend=True,
        legendlabels=['Magnetic data','Satellite data'], grid=True,specialdict={},
        annotate=False,returnfig=False):
    """
    DEFINITION:
        Plot satellite and magnetic data on same plot for storm comparison.
        Currently only plots 1x mag variable vs. 1x sat variable.

    PARAMETERS:
    Variables:
        - mag_stream:   (DataStream object) Stream of magnetic data
        - sat_stream:   (DataStream object) Stream of satellite data
        - keys:         (list) Keys to analyse [mag_key,sat_key], e.g. ['x','y']
    Kwargs:
        - annotate:     (bool) If True, comments in flagged stream lines will be annotated into plot.
        - confinex:     (bool) If True, time strs on y-axis will be confined depending on scale.
        - grid:         (bool) If True, grid will be added to plot. (Doesn't work yet!)
        - legend:       (bool) If True, legend will be added to plot. Default in legendlabels.
        - legendlabels: (list[str]) List of labels to plot in legend.
        - outfile:      (str) Filepath to save plot to.
        - padding:      (float) Padding to add to plotted variables
        - plotcolors:   (list) List of colors for (0) mag data and (1) sat data lines
        - plotfunc:     (bool) If True, fit function will be plotted against sat data.
        - plottitle:    (str) Title to add to plot
        - plottype:     (str) 'discontinuous' (nans will be masked) or 'continuous'.
        - returnfig:    (bool) Return figure object if True
        - savedpi:      (int) DPI of image if plotting to outfile.
        - specialdict:  (dict) Contains limits for plot axes in list form. NOTE this is not the
                        same as other specialdicts. Dict keys should be "sat" and "mag":
                        specialdict = {'mag':[40,100],'sat':[300,450]}

    RETURNS:
        - plot:         (matplotlib plot) A plot of the spectrogram.

    EXAMPLE:
        >>> plotSatMag(LEMI_data, ACE_data, ['x','y'])

    APPLICATION:
        >>>
    """

    logger.info("plotSatMag - Starting plotting of satellite and magnetic data...")

    key_mag, key_sat = keys[0], keys[1]
    ind_mag, ind_sat, ind_t = KEYLIST.index(key_mag), KEYLIST.index(key_sat), KEYLIST.index('time')

    if len(mag_stream.ndarray) > 0.:
        t_mag = mag_stream.ndarray[ind_t]
        t_sat = sat_stream.ndarray[ind_t]
    else:
        t_mag = np.asarray([row[0] for row in mag_stream])
        t_sat = np.asarray([row[0] for row in sat_stream])
    if key_mag not in KEYLIST:
        raise Exception("Column key (%s) not valid!" % key)
    if key_sat not in KEYLIST:
        raise Exception("Column key (%s) not valid!" % key)

    if len(mag_stream.ndarray) > 0.:
        y_mag = mag_stream.ndarray[ind_mag]
        y_sat = sat_stream.ndarray[ind_sat]
    else:
        y_mag = np.asarray([row[ind_mag] for row in mag_stream])
        y_sat = np.asarray([row[ind_sat] for row in sat_stream])

    # Fix if NaNs are present:
    if plottype == 'discontinuous':
        y_mag = maskNAN(y_mag)
        y_sat = maskNAN(y_sat)
    else:
        nans, test = nan_helper(y_mag)
        newt_mag = [t_mag[idx] for idx, el in enumerate(y_mag) if not nans[idx]]
        t_mag = newt_mag
        y_mag = [el for idx, el in enumerate(y_mag) if not nans[idx]]
        nans, test = nan_helper(y_sat)
        newt_sat = [t_sat[idx] for idx, el in enumerate(y_sat) if not nans[idx]]
        t_sat = newt_sat
        y_sat = [el for idx, el in enumerate(y_sat) if not nans[idx]]


    if (len(y_sat) == 0 or len(y_mag)) == 0:
        logger.error("plotSatMag - Can't plot empty column! Full of nans?")
        raise Exception("plotSatMag - Empty column!")

    # Define y-labels:
    try:
        ylabel_mag = mag_stream.header['col-'+key_mag].upper()
    except:
        ylabel_mag = ''
        pass
    try:
        ylabel_sat = sat_stream.header['col-'+key_sat].upper()
    except:
        ylabel_sat = ''
        pass

    try:
        yunit_mag = mag_stream.header['unit-col-'+key_mag]
    except:
        yunit_mag = ''
        pass
    if not yunit_mag == '':
        yunit_mag = re.sub('[#$%&~_^\{}]', '', yunit_mag)
        label_mag = ylabel_mag+' $['+yunit_mag+']$'
    else:
        label_mag = ylabel_mag

    try:
        yunit_sat = sat_stream.header['unit-col-'+key_sat]
    except:
        yunit_sat = ''
        pass
    if not yunit_sat == '':
        yunit_sat = re.sub('[#$%&~_^\{}]', '', yunit_sat)
        label_sat = ylabel_sat+' $['+yunit_sat+']$'
    else:
        label_sat = ylabel_sat

    # PLOT FIGURE
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.set_ylabel(label_sat,color=labelcolor)
    axis1 = ax1.plot_date(t_sat, y_sat, fmt='-', color=plotcolors[1],label=legendlabels[1])

    timeunit = ''
    if confinex:
        tmin = np.min(t_mag)
        tmax = np.max(t_mag)
        # --> If dates to be confined, set value types:
        _confinex(ax1, tmax, tmin, timeunit)
    ax1.set_xlabel("Time (UTC) %s" % timeunit, color=labelcolor)
    if plottitle:
        ax1.set_title(plottitle)

    # NOTE: For mag data to be above sat data in zorder, KEEP THIS AXIS ORDER
    # (twinx() does not play nicely with zorder settings)
    ax2 = ax1.twinx()
    axis2 = ax2.plot_date(t_mag, y_mag, fmt='-', lw=2, color=plotcolors[0],label=legendlabels[0])
    ax2.set_ylabel(label_mag,color=labelcolor)
    ax2.yaxis.set_label_position('left')
    ax2.yaxis.set_ticks_position('left')

    ax1.yaxis.set_label_position('right')
    ax1.yaxis.tick_right()

    # Define y limits:
    if 'mag' in specialdict:
        ax2.set_ylim(specialdict['mag'][0],specialdict['mag'][1])
    else:
        ax2.set_ylim(np.min(y_mag)-padding,np.max(y_mag)+padding)
    if 'sat' in specialdict:
        ax1.set_ylim(specialdict['sat'][0],specialdict['sat'][1])
    else:
        ax1.set_ylim(np.min(y_sat)-padding,np.max(y_sat)+padding)

    # Add a grid:
    # Difficult with a legend and twinx()...
    #if grid:
    #    ax1.grid(zorder=2)
    #    ax2.grid(zorder=1)
    #    ax1.yaxis.grid(False)

    # Add a legend:
    if legend == True:
        axes = axis2 + axis1
        labels = [l.get_label() for l in axes]
        legend = ax1.legend(axes, labels, loc='upper right', shadow=True)
        for label in legend.get_texts():
            label.set_fontsize('small')

    if annotate == True:
        flags = mag_stream.flags
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

    # Plot a function to the satellite data:
    if plotfunc:
        sat_stream._drop_nans('y')
        func = sat_stream.fit(['y'],knotstep=0.02)

        fkey = 'f'+key_sat
        if fkey in func[0]:
            ttmp = arange(0,1,0.0001)
            ax1.plot_date(denormalize(ttmp,func[1],func[2]),func[0][fkey](ttmp),
                '-',color='gray')

    if returnfig == True:
        fig = plt.gcf()
        return fig

    if outfile:
        plt.savefig(outfile,savedpi=80)
        logger.info("plotSatMag - Plot saved to %s." % outfile)
    else:
        plt.show()
        logger.info("plotSatMag - Plot completed.")


def plotSpectrogram(stream, keys, NFFT=1024, detrend=mlab.detrend_none,
             window=mlab.window_hanning, noverlap=900, cmap=defaultcolormap, 
             cbar=False, xextent=None, pad_to=None, sides='default', scale_by_freq=None, 
             minfreq=None, maxfreq=None, plottitle=False, returnfig=False, **kwargs):
    """
    DEFINITION:
        Creates a spectrogram plot of selected keys.
        Parameter description at function obspyspectrogram
        Uses samp_rate_multiplicator (=24*3600): Changes the 
        frequency relative to one day sampling rate given as days ->
        multiplied by x to create Hz,

    PARAMETERS:
    Variables:
        - stream:       (DataStream object) Stream to analyse
        - keys:         (list) Keys to analyse
    Kwargs:
        - cbar:         (bool) Plot colorbar alongside spectrogram
        - returnfig:    (bool) If True, fig object is returned when calling
                        func. Default is False
        --> All other variables are put through to the magpySpecgram
            function and are the same as those used there.
    RETURNS:
        - plot:         (matplotlib plot) A plot of the spectrogram.

    EXAMPLE:
        >>> plotSpectrogram(stream, ['x','y'])

    APPLICATION:
        >>>
    """

    #if not samp_rate_multiplicator:
    samp_rate_multiplicator = 24*3600

    t = stream._get_column('time')

    if not minfreq:
        minfreq = 0.0001

    if not len(t) > 0:
        logger.error('plotSpectrogram: stream of zero length -- aborting')
        return

    for key in keys:
        val = stream._get_column(key)
        val = maskNAN(val)
        dt = stream.get_sampling_period()*(samp_rate_multiplicator)
        Fs = float(1.0/dt)
        if not maxfreq:
            maxfreq = int(Fs/2.0)
            print ("Maxfreq", maxfreq)
            if maxfreq < 1:
                maxfreq = 1
        ax1=subplot(211)
        plt.plot_date(t,val,'-')
        ax1.set_ylabel('{} [{}]'.format(stream.header.get('col-'+key,''),stream.header.get('unit-col-'+key,'')))
        ax1.set_xlabel('Time (UTC)')
        ax2=subplot(212)
        ax2.set_yscale('log')
        #NFFT = 1024
        Pxx, freqs, bins, im = magpySpecgram(val, NFFT=NFFT, Fs=Fs, noverlap=noverlap,
                                cmap=cmap, minfreq = minfreq, maxfreq = maxfreq)
        
        if plottitle:
            ax1.set_title(plottitle)
            
        if cbar:
            fig = plt.gcf()
            axes = fig.axes
            cbar = fig.colorbar(im, ax=np.ravel(axes).tolist())
            cbar.set_label("dB")
        
        if not returnfig:
            plt.show()
        else:
            return fig


def magpySpecgram(x, NFFT=256, Fs=2, Fc=0, detrend=mlab.detrend_none,
             window=mlab.window_hanning, noverlap=128,
             cmap=None, xextent=None, pad_to=None, sides='default',
             scale_by_freq=None, minfreq = None, maxfreq = None, title=False, **kwargs):
    """
    DESCRIPTION
        Compute a spectrogram of data in *x*.  Data are split into
        *NFFT* length segments and the PSD of each section is
        computed.  The windowing function *window* is applied to each
        segment, and the amount of overlap of each segment is
        specified with *noverlap*.
        Taken from http://stackoverflow.com/questions/19468923/cutting-of-unused-frequencies-in-specgram-matplotlib

    APPLICATION:
      specgram(x, NFFT=256, Fs=2, Fc=0, detrend=mlab.detrend_none,
               window=mlab.window_hanning, noverlap=128,
               cmap=None, xextent=None, pad_to=None, sides='default',
               scale_by_freq=None, minfreq = None, maxfreq = None, **kwargs)
    VARIABLE:
    %(PSD)s

      *Fc*: integer
        The center frequency of *x* (defaults to 0), which offsets
        the y extents of the plot to reflect the frequency range used
        when a signal is acquired and then filtered and downsampled to
        baseband.

      *cmap*:
        A :class:`matplotlib.cm.Colormap` instance; if *None* use
        default determined by rc

      *xextent*:
        The image extent along the x-axis. xextent = (xmin,xmax)
        The default is (0,max(bins)), where bins is the return
        value from :func:`mlab.specgram`

      *minfreq, maxfreq*
        Limits y-axis. Both required

      *kwargs*:

        Additional kwargs are passed on to imshow which makes the
        specgram image

      RETURNS:
          Return value is (*Pxx*, *freqs*, *bins*, *im*):

          - *bins* are the time points the spectrogram is calculated over
          - *freqs* is an array of frequencies
          - *Pxx* is a len(times) x len(freqs) array of power
          - *im* is a :class:`matplotlib.image.AxesImage` instance


    Note: If *x* is real (i.e. non-complex), only the positive
    spectrum is shown.  If *x* is complex, both positive and
    negative parts of the spectrum are shown.  This can be
    overridden using the *sides* keyword argument.

    **Example:**

    .. plot:: mpl_examples/pylab_examples/specgram_demo.py

    """

    #####################################
    # modified  axes.specgram() to limit
    # the frequencies plotted
    #####################################

    # this will fail if there isn't a current axis in the global scope
    end = len(x) / Fs

    x = x - mean(x)

    ax = gca()
    Pxx, freqs, bins = mlab.specgram(x, NFFT, Fs, detrend,
         window, noverlap, pad_to, sides, scale_by_freq)

    # modified here
    #####################################
    if minfreq is not None and maxfreq is not None:
        Pxx = Pxx[(freqs >= minfreq) & (freqs <= maxfreq)]
        freqs = freqs[(freqs >= minfreq) & (freqs <= maxfreq)]
    #####################################

    Z = 10. * np.log10(Pxx)
    Z = np.flipud(Z)

    if xextent is None: xextent = 0, np.amax(bins)
    xmin, xmax = xextent
    freqs += Fc
    extent = xmin, xmax, freqs[0], freqs[-1]
    im = ax.imshow(Z, cmap, extent=extent, **kwargs)
    ax.axis('auto')

    # set correct way of axis, whitespace before and after with window
    # length
    ax.axis('tight')
    ax.set_xlim(0, end)
    ax.grid(False)

    ax.set_xlabel('Time [s]')
    ax.set_ylabel('Frequency [Hz]')
    if title:
        ax.set_title(title)

    return Pxx, freqs, bins, im


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
        - stream        (DataStream) a magpy datastream object
    Kwargs:
        - bgcolor:      (colour='#d5de9c') Background colour
        - figure:       (bool) Show figure if True
        - focus:        (str) defines the plot area. Options:
                        all (default) - -90 to 90 deg inc, 360 deg dec
                        q1 - first quadrant
                        q2 - first quadrant
                        q3 - first quadrant
                        q4 - first quadrant
                        data - focus on data (if angular spread is less then 10 deg
        - gridcolor:    (str) Define grid color e.g. '0.5' greyscale, 'r' red, etc
        - griddeccolor: (colour='#316931') Grid colour for inclination
        - gridinccolor: (colour='#316931') Grid colour for declination
        - groups        (KEY) - key of keylist which defines color of points
                        (e.g. ('str2') in absolutes to select
                        different colors for different instruments
        - legend:       (bool) - draws legend only if groups is given - default True
        - legendposition:
                        (str) - draws the legend at chosen position,
                        (e.g. "upper right", "lower center") - default is "lower left"
        - labellimit:   (int)- maximum length of label in legend
        - noshow:       (bool) If True, will not call show at the end,
        - outfile:      (str) to save the figure, if path is not existing it will be created
        - savedpi:      (int) resolution
        - plottitle:    (str) Title at top of plot

    REQUIRES:
        - package operator for color selection

    RETURNS:
        - plot:         (matplotlib plot) The stereoplot.

            ToDo:
                - add alpha 95 calc

    EXAMPLE:
        >>> stream.stereoplot(focus='data',groups='str2')

    APPLICATION:
        >>>
    """

    logger.info('plotStereoplot: Starting plot of stereoplot.')

    if not stream[0].typ == 'idff':
        logger.error('plotStereoplot: idf data required for stereoplot.')
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
        logger.error('plotStereoplot: Check input file - unequal inc and dec data?')
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
#                                                                   #
#       INTERNAL/HELPER FUNCTIONS                                   #
#       (Best not play with these.)                                 #
#                                                                   #
#####################################################################


def _plot(data,savedpi=80,grid=True,gridcolor=gridcolor,noshow=False,
        bgcolor='white',plottitle=None,fullday=False,bartrange=0.06,
        labelcolor=labelcolor,confinex=False,outfile=None,stormanno_s=True,
        stormanno_m=True,stormanno_r=True,fmt=None,figure=False,fill=[],
        legendposition='upper left',singlesubplot=False,opacity=1.0):
    '''
    For internal use only. Feed a list of dictionaries in here to plot.
    Every dictionary should contain all data needed for one single subplot.
    DICTIONARY STRUCTURE FOR EVERY SUBPLOT:
    [ { ***REQUIRED***
        'key'   : 'x'           (str) MagPy key
        'tdata' : t             (np.ndarray) Time
        'ydata' : y             (np.ndarray) Data y(t)
        'ymin'  : ymin          (float)    Minimum of y-axis
        'ymax'  : ymax          (float)    Maximum of y-axis
        'symbol': '-'           (str)      Symbol for plotting, '-' = line
        'color' : 'b'           (str)      Colour of plotted line
        'ylabel': 'F [nt]'      (str)      Label on y-axis
        'annotate': False       (bool)     If this is True, must have 'flags' key
        'sensorid': 'LEMI025'   (str)      String pulled from header data. If available,
                                will be plotted alongside data for clarity.

        OPTIONAL:
        'errorbars': eb         (np.ndarray) Errorbars to plot in subplot
        'flags' : flags         (np.ndarray) Flags to add into subplot.
                                Note: must be 2-dimensional, flags & comments.
        'function': fn          (function object) Plot a function within the subplot.
        } ,

      { 'key' : ...                             } ... ]

    GENERAL VARIABLES:
    plottitle = "Data from 2014-05-02"
    confinex = False
    bgcolor = 'blue'
    etc. ... (all are listed in plot() and plotStreams() functions)
    figure  -- for GUI
    fill = ['x']
    '''
    logger = logging.getLogger(__name__+"._plot")

    if not figure:
        fig = plt.figure()
    else:
        fig = figure

    # CREATE MATPLOTLIB FIGURE OBJECT:
    #fig = plt.figure()
    plt_fmt = ScalarFormatter(useOffset=False)
    n_subplots = len(data)

    for i in range(n_subplots):

        subplt = "%d%d%d" %(n_subplots,1,i+1)
        if singlesubplot:
            subplt = "111"

        #------------------------------------------------------------
        # PART 1: Dealing with data
        #------------------------------------------------------------

        # DEFINE DATA:
        key = data[i]['key']
        t = np.asarray(data[i]['tdata']).astype(float)
        y = np.asarray(data[i]['ydata']).astype(float)
        if not len(t) == len(y):
            y = [99999]*len(t)
        # Sort data before plotting - really necessary ? costs 0.1 seconds for 1 day second data
        #datar = sorted([[t[j],y[j]] for j, el in enumerate(t)])
        #t = [datar[j][0] for j, el in enumerate(datar)]
        #y = [datar[j][1] for j, el in enumerate(datar)]
        color = data[i]['color']
        symbol = data[i]['symbol']
        datalabel = data[i]['datalabel']

        # CREATE SUBPLOT OBJECT & ADD TITLE:
        logger.info("Adding subplot for key %s..." % data[i]['ylabel'])
        if i == 0:
            ax = fig.add_subplot(subplt)#, axisbg=bgcolor)
            ax.patch.set_facecolor(bgcolor)
            if plottitle:
                ax.set_title(plottitle)
            a = ax
        else:
            ax = fig.add_subplot(subplt, sharex=a) #, axisbg=bgcolor)
            ax.patch.set_facecolor(bgcolor)

        # PLOT DATA:
        # --> If bars are in the data (for e.g. k-index):
        if symbol == 'z':
            xy = range(9)
            for num in range(len(t)):
                if bartrange < t[num] < np.max(t)-bartrange:
                    ax.fill([t[num]-bartrange,t[num]+bartrange,t[num]+bartrange,t[num]-
                                bartrange],[0,0,y[num]+0.1,y[num]+0.1],
                                facecolor=cm.RdYlGn((9-y[num])/9.,1),alpha=opacity,edgecolor='k')
            if datalabel != '':
                ax.plot_date(t,y,color+'|',label=datalabel,alpha=opacity)
            else:
                ax.plot_date(t,y,color+'|',alpha=opacity)

        # --> Otherwise plot as normal:
        else:
            if datalabel != '':
                ax.plot_date(t,y,color+symbol,label=datalabel)
            else:
                ax.plot_date(t,y,color+symbol)
            if key in fill:
                ax.fill_between(t,0,y,color=color,alpha=opacity)

        # PLOT A LEGEND
        if datalabel != '':
            legend = ax.legend(loc=legendposition, shadow=True)
            for label in legend.get_texts():
                label.set_fontsize('small')

        # DEFINE MIN AND MAX ON Y-AXIS:
        ymin = data[i]['ymin']
        ymax = data[i]['ymax']

        # PLOT ERROR BARS (if available):
        if 'errors' in data[i]:
            errorbars = data[i]['errors']
            ax.errorbar(t,y,yerr=errorbars,fmt=color+'o')

        # ANNOTATE:
        if data[i]['annotate'] == True:
            orientationcnt = 0
            flags = data[i]['flags']
            emptycomment = "-"
            indexflag = KEYLIST.index(key)
            # identfy subsequent idx nums in flags[1]
            a_t,a_y,b_t,b_y,c_t,c_y,d_t,d_y = [],[],[],[],[],[],[],[]
            if len(flags[1]) > 0:
                # 1. get different comments
                tmp = DataStream()
                uniqueflags = tmp.union(flags[1])
                #print "Flags", flags,uniqueflags, key
                for fl in uniqueflags:
                    #print ("Flag", fl)
                    flagindicies = []
                    for idx, elem in enumerate(flags[1]):
                        if not elem == '' and elem == fl:
                            #print ("ELEM", elem)
                            flagindicies.append(idx)
                    #print "IDX", np.asarray(flagindicies)
                    # 2. get consecutive groups
                    for k, g in groupby(enumerate(flagindicies), lambda ix: ix[0] - ix[1]):
                        orientationcnt += 1 # used to flip orientation of text box
                        consecutives = list(map(itemgetter(1), g))
                        # 3. add annotation arrow for all but 1
                        cnt0 = consecutives[0]
                        #print ("Cons", np.asarray(consecutives), len(flags[0][cnt0]))
                        if len(flags[0][cnt0]) >= indexflag:
                          try:
                            #print ("Fl", flags[0][cnt0][indexflag], flags[1][cnt0], y[cnt0])
                            if not flags[0][cnt0][indexflag] in ['1','-'] and not flags[1][cnt0] == '-':
                                axisextend = (max(y)-min(y))*0.15 ## 15 percent of axislength
                                if orientationcnt % 2 or y[cnt0]-axisextend < min(y):
                                    xytext=(20, 20)
                                elif y[cnt0]+axisextend > max(y):
                                    xytext=(20, -20)
                                else:
                                    xytext=(20, -20)
                                connstyle = "angle,angleA=0,angleB=90,rad=10"
                                ax.annotate(r'%s' % (flags[1][cnt0]),
                                        xy=(t[cnt0], y[cnt0]),
                                        xycoords='data', xytext=xytext, size=10, 
                                        textcoords='offset points',
                                        bbox=dict(boxstyle="round", fc="0.9"),
                                        arrowprops=dict(arrowstyle="->",
                                        shrinkA=0, shrinkB=1, connectionstyle=connstyle))
                            for idx in consecutives:
                                #if not flags[0][idx][indexflag] == '0':
                                #    print "Got", flags[0][idx][indexflag], idx
                                if flags[0][idx][indexflag] in ['3']:
                                    a_t.append(float(t[idx]))
                                    a_y.append(y[idx])
                                elif flags[0][idx][indexflag] in ['1']:
                                    b_t.append(float(t[idx]))
                                    b_y.append(y[idx])
                                elif flags[0][idx][indexflag] in ['2']:
                                    c_t.append(float(t[idx]))
                                    c_y.append(y[idx])
                                elif flags[0][idx][indexflag] in ['4']:
                                    d_t.append(float(t[idx]))
                                    d_y.append(y[idx])
                          except:
                            logger.error("Error when marking flags - check: {} {}".format(flags[0][cnt0], indexflag))
                        else:
                            logger.info("Found problem in flagging information - still to be solved")
                            logger.info("Flag at count and its index position {} {}".format(cnt0, indexflag))
                            logger.info("Flag and Comment (expected -000000000 and comment) {} {}".format(flags[0][cnt0], flags[1][cnt0]))
                linecrit = 2000
                if len(a_t) > 0:
                    if len(a_t) > linecrit:
                        ax.plot(a_t,a_y,'.',c='r') ## Use lines if a lot of data is marked
                    else:
                        ax.scatter(a_t,a_y,c='r')
                if len(b_t) > 0:
                    if len(b_t) > linecrit:
                        ax.plot(b_t,b_y,'.',c='orange')
                    else:
                        ax.scatter(b_t,b_y,c='orange')
                if len(c_t) > 0:
                    # TODO Here we have a masked nan warning - too be solved
                    #print np.asarray(c_t)
                    #print np.asarray(c_y)
                    if len(c_t) > linecrit:
                        ax.plot(c_t,c_y,'.',c='g')
                    else:
                        ax.scatter(c_t,c_y,c='g')
                if len(d_t) > 0:
                    if len(d_t) > linecrit:
                        ax.plot(d_t,d_y,'.',c='b')
                    else:
                        ax.scatter(d_t,d_y,c='b')

        # PLOT A GIVEN FUNCTION:
        if 'function' in data[i]:
            fkey = 'f'+key
            funclist = data[i]['function']
            if isinstance(funclist[0], dict):
                 funct = [funclist]
            else:
                 funct = funclist   # TODO: cycle through list
            for function in funct:
                for nu in range(int(len(function)/3.)):
                    indexadd = nu*3
                    if fkey in function[0+indexadd]:
                        # --> Get the minimum and maximum relative times
                        ttmp = arange(0,1,0.0001)
                        ax.plot_date(denormalize(ttmp,function[1+indexadd],function[2+indexadd]),function[0+indexadd][fkey](ttmp),'r-')

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
        

def _extract_data_for_PSD(stream, key):
    """
    Prepares data for power spectral density evaluation.
    """
    
    if len(stream.ndarray[0]) > 0:
        pos = KEYLIST.index(key)
        t = stream.ndarray[0]
        val = stream.ndarray[pos]
    else:
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
    
    return t_new, val_new, nfft


#####################################################################
#                                                                   #
#       TESTING                                                     #
#       Run this after making changes:                              #
#       $ python mpplot.py                                          #
#                                                                   #
#####################################################################

if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: PLOTTING PACKAGE")
    print("All plotting methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print()
    print("NOTE: This test requires graphical user interface")
    print("confirmation of package integrity for the majority of")
    print("functions. The plot titles specify what should be present")
    print("in the plot for it to have plotted successfully.")
    print("    So get comfy and have a good look.")
    print("----------------------------------------------------------")
    print()

    print("Please enter path of a variometer data file for testing:")
    print("(e.g. /srv/archive/WIC/LEMI025/LEMI025_2014-05-07.bin)")
    while True:
        filepath = raw_input("> ")
        if os.path.exists(filepath):
            break
        else:
            print("Sorry, that file doesn't exist. Try again.")
    print()

    now = datetime.utcnow()
    testrun = 'plottest_'+datetime.strftime(now,'%Y%m%d-%H%M')
    t_start_test = time.time()
    errors = {}

    print(datetime.utcnow(), "- Starting plot package test. This run: %s." % testrun)

    while True:

        # Step 1 - Read data
        try:
            teststream = read(filepath,tenHz=True)
            print(datetime.utcnow(), "- Stream read in successfully.")
        except Exception as excep:
            errors['read'] = str(excep)
            print(datetime.utcnow(), "--- ERROR reading stream. Aborting test.")
            break

        # Step 2 - Pick standard key for all other plots
        try:
            keys = teststream._get_key_headers()
            key = [keys[0]]
            key2 = [keys[0], keys[1]]
            print(datetime.utcnow(), "- Using %s key for all subsequent plots." % key[0])
        except Exception as excep:
            errors['_get_key_headers'] = str(excep)
            print(datetime.utcnow(), "--- ERROR getting default keys. Aborting test.")
            break

        # Step 3 - Simple single plot with ploteasy
        try:
            ploteasy(teststream)
            print(datetime.utcnow(), "- Plotted using ploteasy function.")
        except Exception as excep:
            errors['ploteasy'] = str(excep)
            print(datetime.utcnow(), "--- ERROR with ploteasy function. Aborting test.")
            break

        # Step 4 - Standard plot
        try:
            plot_new(teststream,key,
                        plottitle = "Simple plot of %s" % key[0])
            print(datetime.utcnow(), "- Plotted standard plot.")
        except Exception as excep:
            errors['plot-vanilla'] = str(excep)
            print(datetime.utcnow(), "--- ERROR with standard plot. Aborting test.")
            break

        # Step 5 - Multiple streams
        streamlist =    [teststream,    teststream      ]
        variables =     [key,           key2            ]
        try:
            plotStreams(streamlist, variables,
                        plottitle = "Multiple streams: Three bars, top two should match.")
            print(datetime.utcnow(), "- Plotted multiple streams.")
        except Exception as excep:
            errors['plotStreams-vanilla'] = str(excep)
            print(datetime.utcnow(), "--- ERROR with plotting multiple streams. Aborting test.")
            break

        # Step 6 - Normalised stream comparison
        try:
            plotNormStreams([teststream], key[0],
                        confinex = True,
                        plottitle = "Normalized stream: Stream key should be normalized to zero.")
            print(datetime.utcnow(), "- Plotted normalized streams.")
        except Exception as excep:
            errors['plotNormStreams'] = str(excep)
            print(datetime.utcnow(), "--- ERROR plotting normalized streams.")

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
            print(datetime.utcnow(), "- Plotted annotated single plot of storm phases.")
        except Exception as excep:
            errors['plot-stormphases'] = str(excep)
            print(datetime.utcnow(), "--- ERROR with storm phases plot.")

        # Step 8b - Plot with phases (multiple)
        try:
            plotStreams(streamlist,variables,
                        stormphases = True,
                        t_stormphases = t_stormphases,
                        plottitle = "Multiple plot showing all THREE storm phases, annotated")
            print(datetime.utcnow(), "- Plotted annotated multiple plot of storm phases.")
        except Exception as excep:
            errors['plotStreams-stormphases'] = str(excep)
            print(datetime.utcnow(), "--- ERROR with storm phases multiple plot.")

        # Step 9 - Plot satellite vs. magnetic data
        try:
            xmin, xmax = np.min(teststream._get_column('x')), np.max(teststream._get_column('x'))
            ymin, ymax = np.min(teststream._get_column('y')), np.max(teststream._get_column('y'))
            plotSatMag(teststream,teststream,['x','y'],
                        specialdict={'mag':[xmin-45,xmax+5],'sat':[ymin-5,ymax+45]},
                        plottitle = "Two variables in same plots with double y axes")
            print(datetime.utcnow(), "- Plotted magnetic/satellite data.")
        except Exception as excep:
            errors['plotSatMag'] = str(excep)
            print(datetime.utcnow(), "--- ERROR with plotSatMagplot.")

        # Step 10 - Plot power spectrum
        try:
            freqm, asdm = plotPS(teststream,key[0],
                        returndata=True,
                        marks={'Look here!':0.0001, '...and here!':0.01},
                        plottitle = "Simple power spectrum plot with two marks")
            print(datetime.utcnow(), "- Plotted power spectrum. Max frequency is at %s." % max(freqm))
        except Exception as excep:
            errors['plotPS'] = str(excep)
            print(datetime.utcnow(), "--- ERROR plotting power spectrum.")

        # Step 11 - Plot normal spectrogram
        try:
            plotSpectrogram(teststream,key2,
                        plottitle = "Spectrogram of two keys")
            print(datetime.utcnow(), "- Plotted spectrogram.")
        except Exception as excep:
            errors['plotSpectrogram'] = str(excep)
            print(datetime.utcnow(), "--- ERROR plotting spectrogram.")

        # Step 12 - Plot function
        try:
            func = teststream.fit(key,knotstep=0.02)
            plot_new(teststream,key,function=func,
                        plottitle = "Fit function plotted over original data.")
        except Exception as excep:
            errors['plot(function)'] = str(excep)
            print(datetime.utcnow(), "--- ERROR plotting function.")

        # Step 13 - Plot normal stereoplot
        # (This should stay as last step due to coordinate conversion.)
        try:
            teststream._convertstream('xyz2idf')
            plotStereoplot(teststream,
                        plottitle="Standard stereoplot")
            print(datetime.utcnow(), "- Plotted stereoplot.")
        except Exception as excep:
            errors['plotStereoplot'] = str(excep)
            print(datetime.utcnow(), "--- ERROR plotting stereoplot.")

        # If end of routine is reached... break.
        break

    t_end_test = time.time()
    time_taken = t_end_test - t_start_test
    print(datetime.utcnow(), "- Stream testing completed in %s s. Results below." % time_taken)

    print()
    print("----------------------------------------------------------")
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(str(errors.keys()))
        print()
        print("Would you like to print the exceptions thrown?")
        excep_answer = raw_input("(Y/n) > ")
        if excep_answer.lower() == 'y':
            i = 0
            for item in errors:
                print(errors.keys()[i] + " error string:")
                print("    " + errors[errors.keys()[i]])
                i += 1
    print()
    print("Good-bye!")
    print("----------------------------------------------------------")

