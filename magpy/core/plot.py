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
                        function=function,plottype=plottype,flagontop=flagontop,resolution=resolution, **kwargs)


def plot(stream,variables=[],specialdict={},errorbars=False,padding=0,noshow=False,
        annotate=False,stormphases=False,colorlist=colorlist,symbollist=symbollist,
        t_stormphases=None,includeid=False,flagontop=False,function=None,plottype='discontinuous',resolution=None,
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
        errorbars=errorbars,padding=padding,annotate=annotate,flagontop=flagontop,stormphases=stormphases,
        colorlist=colorlist,symbollist=symbollist,t_stormphases=t_stormphases,
        includeid=includeid,function=function,plottype=plottype,resolution=resolution,**kwargs)


def plotStreams(streamlist,variables,padding=None,specialdict={},errorbars=None,
        colorlist=colorlist,symbollist=symbollist,annotate=None,stormphases=None,
        t_stormphases={},includeid=False,function=None,plottype='discontinuous',
        noshow=False,labels=False,flagontop=False,resolution=None,debug=False,**kwargs):
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
        - flagontop:    (True/False) define whether flags are shown above or below data.
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
        orgt = np.asarray([ti for ti in t]) # orgt is needed if time column is modified (i.e. continuous plots)
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

            if len(y) == 0:
                logger.error("Cannot plot stream of zero length!")

            # eventually remove flagged:
            dropflagged = False
            if dropflagged:
                flagind = KEYLIST.index('flag')
                flags = stream.ndarray[flagind]
                ind = KEYLIST.index(key)
                flagarray =  np.asarray([list(el)[ind] for el in flags])
                if debug:
                    print("Flagarray", flagarray)
                indicies = np.where(flagarray == '1')
                if debug:
                    print("Indicis", indicis)

            # Fix if NaNs are present:
            if plottype == 'discontinuous':
                y = maskNAN(y)
            else:
                nans, test = nan_helper(y)
                newt = [orgt[idx] for idx, el in enumerate(y) if not nans[idx]]
                t = newt
                y = [el for idx, el in enumerate(y) if not nans[idx]]

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
                data_dict['ymin'] = np.min(y) - ypadding
                data_dict['ymax'] = np.max(y) + ypadding
                if key in specialdict[i]:
                    specialparams = specialdict[i][key]
                    try:
                        if DataStream()._is_number(specialparams[0]):
                            lowlev = specialparams[0]
                        else:
                            lowlev = np.min(y)
                    except:
                        lowlev = np.min(y)
                    try:
                        if DataStream()._is_number(specialparams[1]):
                            highlev = specialparams[1]
                        else:
                            highlev = np.max(y)
                    except:
                        highlev = np.max(y)
                    data_dict['ymin'] = lowlev - ypadding
                    data_dict['ymax'] = highlev + ypadding
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

            data_dict['flagontop'] = flagontop

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
#                                                                   #
#       INTERNAL/HELPER FUNCTIONS                                   #
#       (Best not play with these.)                                 #
#                                                                   #
#####################################################################


def _plot(data,savedpi=80,grid=True,gridcolor=gridcolor,noshow=False,
        bgcolor='white',plottitle=None,fullday=False,bartrange=0.06,
        labelcolor=labelcolor,confinex=False,outfile=None,fmt=None,figure=False,fill=[],
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
        'flagontop': True/False  (bool) define whether flags are on top or not.
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
    zorder = 2

    for i in range(n_subplots):

        subplt = int("%d%d%d" %(n_subplots,1,i+1))
        if singlesubplot:
            subplt = int("111")

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

        if 'flagontop' in data[i]:
            if data[i]['flagontop'] == True:
                zorder = 10
            else:
                zorder = 2

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

        if datalabel != '':
            ax.plot(t,y,color+symbol,label=datalabel)
        else:
            ax.plot(t,y,color+symbol)
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
                        ax.plot(a_t,a_y,'.',c='r', zorder=zorder) ## Use lines if a lot of data is marked
                    else:
                        ax.scatter(a_t,a_y,c='r', zorder=zorder)
                if len(b_t) > 0:
                    if len(b_t) > linecrit:
                        ax.plot(b_t,b_y,'.',c='orange', zorder=zorder)
                    else:
                        ax.scatter(b_t,b_y,c='orange', zorder=zorder)
                if len(c_t) > 0:
                    # TODO Here we have a masked nan warning - too be solved
                    #print np.asarray(c_t)
                    #print np.asarray(c_y)
                    if len(c_t) > linecrit:
                        ax.plot(c_t,c_y,'.',c='g', zorder=zorder)
                    else:
                        ax.scatter(c_t,c_y,c='g', zorder=zorder)
                if len(d_t) > 0:
                    if len(d_t) > linecrit:
                        ax.plot(d_t,d_y,'.',c='b', zorder=zorder)
                    else:
                        ax.scatter(d_t,d_y,c='b', zorder=zorder)

        # PLOT A GIVEN FUNCTION:
        if 'function' in data[i]:
            fkey = 'f'+key
            funclist = data[i]['function']
            if isinstance(funclist[0], dict):
                 funct = [funclist]
            else:
                 funct = funclist
            for function in funct:
                # length of function - an old version just added up functions in one list
                funclength = len(function)
                for nu in range(int(len(function)/funclength)):
                    indexadd = nu*funclength
                    if fkey in function[0+indexadd]:
                        # --> Get the minimum and maximum relative times
                        ttmp = arange(0,1,0.0001)
                        ax.plot_date(denormalize(ttmp,function[1+indexadd],function[2+indexadd]),function[0+indexadd][fkey](ttmp),'r-')

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
    #fig.subplots_adjust(hspace=0)

    # ADJUST X-AXIS FOR FULLDAY PLOTTING:
    if fullday:
        ax.set_xlim(np.floor(np.round(np.min(t)*100)/100),np.floor(np.max(t)+1))

    # SAVE OR SHOW:
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
