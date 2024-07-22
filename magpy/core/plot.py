
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import *

logger = logging.getLogger(__name__)

import matplotlib

gui_env = ['TKAgg', 'GTKAgg', 'Agg'] # remove WXAgg and add this activation to GUI
maversion = matplotlib.__version__.replace('svn', '')
try:
    maversion = map(int, maversion.replace("rc", "").split("."))
    matplotlibversion = list(maversion)
except:
    maversion = maversion.strip("rc")
    matplotlibversion = maversion
logger.info("Loaded Matplotlib - Version %s" % str(matplotlibversion))

noterminal = False
try:
        if not os.isatty(sys.stdout.fileno()):   # checks if stdout is connected to a terminal (if not, cron is starting the job)
            logger.info("No terminal connected - assuming cron job and using Agg for matplotlib")
            #gui_env = ['WXAgg','TKAgg','Agg','GTKAgg','Qt4Agg']
            matplotlib.use('Agg') # For using cron
            notermial = True
except:
        logger.warning("Problems with identfying cron job - windows system?")
        pass
if not noterminal:
    for gui in gui_env:
        try:
            logger.info("Testing backend {}".format(gui))
            try:  # will be important from matplotlib3.3 onwards
                matplotlib.use(gui, force=True)
            except:
                matplotlib.use(gui, warn=False, force=True)
            from matplotlib import pyplot as plt
            break
        except:
            continue
logger.info("Using backend: {}".format(matplotlib.get_backend()))

#from matplotlib.colors import Normalize
#from matplotlib.widgets import RectangleSelector, RadioButtons
#from matplotlib import mlab
from matplotlib.dates import date2num, num2date
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import matplotlib.patches as patches

edgecolor = [0.8, 0.8, 0.8]


def testtimestep(variable):
    try:
        if isinstance(variable, (datetime, datetime64)):
            return True
        else:
            return False
    except:
        return False


def fill_list(mylist, target_len, value):
    return mylist[:target_len] + [value] * (target_len - len(mylist))


def tsplot(data=[DataStream()], keys=[['dummy']], timecolumn=['time'], xrange=None, yranges=None, padding=None,
           symbols=None, symbolcolor=[[0.8, 0.8, 0.8]], title=None, xinds=[None], legend={}, grid={}, patch={},
           fill=None, showpatch=[True], width=10, height=4, labelx=None, alpha=0.5, debug=False):
    """
    DESCRIPTION:
        tsplot creates a timeseries plot of selected data
        will replace plotStreams, ploteasy and plot of magpy1.x

        plotStreams(errorbars=None, annotate=None, function=None, plottype='discontinuous', noshow=False, **kwargs)

    OPTIONS:
        keys (list)         :    default are the first three available keys of the first data set. If only keys for the first data set are given, then an overlay plot is activated
                                 If all keys are provided then all plots are plotted separately.
        symbols (list)      :    default "-" : define plot symbols - accpeted are "-",".","o" and "k"
        symbolcolor (list)  :    default is light grey : defines the color of all individual plots of each datastream
        timecolumn (list)   :    default None : define the used time column - can be time or sectime (if existing)
        yranges (list)      :    default None : define individual ranges for y axes - overrides padding
                                 EXAMPLE: keys=[['x','y'],['f']], yranges=[[[20000,22000],[-1000,1000]],[[44000,54000]]]
        padding (list)      :    default None : define scalar paddings to minimal and maximal data value for y range
                                 EXAMPLE: keys=[['x','y'],['f']], padding=[[100,50],[10]]
        fill (dict)         :    provide a list with specific fill information (dicts) for each key i.e. [{"boundary":400,"fillcolor":"red"},{"boundary":400,"fillcolor":"blue","fillrange":"smaller"}]
                                 to fill everything above 400 read and everything below 400 blue for key y
                                 EXAMPLE: keys=[['x','y','z']], fill=[[[],[{"boundary":400,"fillcolor":"red"},{"boundary":400,"fillcolor":"blue","fillrange":"smaller"}],[{"boundary":0,"fillcolor":"red"}]]]
        legend (dict)       :    default None
                                 EXAMPLE: legend={"legendtext":('Primary-LEMI036_2', 'Primary-FGE_S0252'),"legendposition":"upper right","legendstyle":"shadow","plotnumber":2}
                                 or legend=True for default with SensorID
        grid (dict)         :    default None
                                 EXAMPLE: grid={"visible":True,"which":"major","axis":"both","color":"k"}
                                 or grid=True  for default values
        patch (dict)        :    default none - patch contain colored regions covering the full vertical space in each plot - used for flagging info
                                 {"patch1":{"start":datetime,"end":datetime,"color":color,"alpha":0.2},"patch2":{"start":datetime,"end":datetime,"color":color,"alpha":0.2}]
        showpatch (list)    :    default True for all streams -
                                 EXAMPLE: data=[stream1,stream2,stream3],showpatches=[True,False,True]
        labelx (float)      :    default none - if defined then all ylabels are set to this x position
                                 EXAMPLE: labelx=-0.1
        height (float)      :    default 4 - default height of each individual plot
                                 EXAMPLE: height=2
        width (float)       :    default 10 - default width of all plots
                                 EXAMPLE: width=12

    EXAMPLE:
        Overlay plot of two difference plots with x,y,z respectively
        p = tsplot([delta1,delta2],keys=[['x','y','z']], yranges=[[[-5,5],[-5,5],[-5,5]]], symbolcolor=[[0.8, 0.8, 0.8],[0.5, 0.5, 0.5]], height=2, legend={"legendtext":('Primary-LEMI036_2', 'Primary-FGE_S0252'),"legendposition":"upper right","legendstyle":"shadow","plotnumber":2})
        Fill options:
        p = tsplot(ace1,fill=[[[],[{"boundary":400,"fillcolor":"red"},{"boundary":400,"fillcolor":"blue","fillrange":"smaller"}],[{"boundary":0,"fillcolor":"red"}]]])
    """
    # This method requires the porovision of data sets on which the get_gaps method has been applied
    titledone = False
    # check if provided data is a list
    if not isinstance(data, (list, tuple)):
        data = [data]
        # amount of plots
    amount = len(data)
    # check for available keys - do that only for the primary dataset if not provided
    if keys:
        keysdepth = len(np.array(keys, dtype=object).shape)
        if keysdepth == 1 and amount == 1:
            keys = [keys]
    if keys and keys[0][0] == 'dummy':
        keys = [data[0]._get_key_headers(limit=3)]
    if len(keys) < amount or amount == 1:
        hght = int(height * len(keys[0]))
        separate = False
    else:
        hght = int(height * np.concatenate(keys).size)
        separate = True
    if not symbols:
        symbols = [["-" for el in line] for line in keys]
    if not yranges:
        yranges = [[["default", "default"] for el in line] for line in keys]
    keys = fill_list(keys, amount, keys[-1])
    symbols = fill_list(symbols, amount, symbols[-1])
    yranges = fill_list(yranges, amount, yranges[-1])
    timecolumn = fill_list(timecolumn, amount, timecolumn[-1])
    showpatch = fill_list(showpatch, amount, showpatch[-1])
    symbolcolor = fill_list(symbolcolor, amount, symbolcolor[-1])
    xinds = fill_list(xinds, amount, xinds[-1])
    if not title:
        title = [None]
    if isinstance(title, (list, tuple)):
        title = fill_list(title, amount, title[-1])
    if yranges:
        if len(yranges[0]) == len(keys[0]):
            yranges = fill_list(yranges, amount, yranges[-1])
        else:
            yranges = None
    if padding:
        if len(padding[0]) == len(keys[0]):
            padding = fill_list(padding, amount, padding[-1])
        else:
            padding = None

    plt.figure(figsize=(width, hght))

    # parameter for separate plots
    total_pos = 0
    total_keys = np.concatenate(keys).size

    for idx, dat in enumerate(data):
        # x column
        x = dat._get_column(timecolumn[idx])
        if len(x) > 1 and testtimestep(x[0]):
            t = x
            # 0. date directly (~0 sec)
        else:
            t = num2date(x)
        if not xinds[idx]:
            # plot only selected indicies
            xinds[idx] = list(range(0, len(x), 1))

        for i, component in enumerate(keys[idx]):
            comp = dat._get_column(component)
            if separate:
                subplot = int("{}1{}".format(total_keys, total_pos + 1))
            else:
                subplot = int("{}1{}".format(len(keys[idx]), i + 1))
            ax = plt.subplot(subplot)

            symbol = symbols[idx][i]
            if symbol == "k":
                # special symbol for K values
                diffs = (np.asarray(t[1:].astype(datetime64) - t[:-1].astype(datetime64)) / 1000000.).astype(
                    float64)  # in seconds
                diffs = diffs[~np.isnan(diffs)]
                me = np.median(diffs)
                bartrange = timedelta(seconds=(me / 2.) * 0.95)  # use 99% of the half distance for bartrange
                xy = range(9)
                for num in range(len(t)):
                    # if bartrange < t[num] < np.max(t)-bartrange:
                    ax.fill([t[num] - bartrange, t[num] + bartrange, t[num] + bartrange, t[num] -
                             bartrange], [0, 0, comp[num] + 0.1, comp[num] + 0.1],
                            facecolor=cm.RdYlGn((9 - comp[num]) / 9., 1), alpha=alpha, edgecolor='k')
                symbol = "|"
                symbolcolor[idx] = None
                yranges[idx][i] = [0, 9]
            # fill between graphs
            if fill:
                try:
                    fdi = fill[idx][i]
                except:
                    fdi = 0
                if isinstance(fdi, (list, tuple)):
                    for fdiitem in fdi:
                        boundary = fdiitem.get("boundary", 0)
                        fillcolor = fdiitem.get("fillcolor", "gray")
                        fillalpha = fdiitem.get("fillaplha", alpha)
                        fillrange = fdiitem.get("fillrange", "larger")
                        if fillrange == "larger":
                            indi = np.where(comp > boundary, True, False)
                        else:
                            indi = np.where(comp < boundary, True, False)
                        ax.fill_between(t, comp, boundary, color=fillcolor, alpha=alpha, where=indi)
            # Plot the main graph
            plt.plot(t, comp, symbol, color=symbolcolor[idx])
            # plt.hlines(0,t[0],t[-1])
            # Define y ranges
            mincomp = np.nanmin(comp)
            maxcomp = np.nanmax(comp)
            if yranges:
                adjustrange = False
                if not yranges[idx][i][0] == "default":
                    adjustrange = True
                    mincomp = yranges[idx][i][0]
                if not yranges[idx][i][1] == "default":
                    adjustrange = True
                    maxcomp = yranges[idx][i][1]
                if adjustrange:
                    plt.ylim(mincomp, maxcomp)
            elif padding:
                mincomp = mincomp - padding[idx][i]
                maxcomp = maxcomp + padding[idx][i]
                plt.ylim(mincomp, maxcomp)

            # Plot patches - for flagging etc
            if isinstance(patch, list):
                try:
                    pat = patch[idx][i]
                except:
                    pat = {}
            else:
                pat = patch
            if pat and isinstance(pat, dict) and showpatch[idx]:
                for line in pat:
                    l = pat.get(line)
                    winmin = date2num(l.get('start'))
                    winmax = date2num(l.get('end'))
                    edgecolor = l.get('color')
                    if not edgecolor:
                        if l.get('flagtype', 0) in [1, 3]:
                            edgecolor = 'r'
                        elif l.get('flagtype', 0) in [2, 4]:
                            edgecolor = 'g'
                        elif l.get('flagtype', 0) == 0:
                            edgecolor = 'y'
                    rect = patches.Rectangle((winmin, mincomp), winmax - winmin, maxcomp - mincomp, edgecolor=edgecolor,
                                             facecolor=edgecolor, alpha=0.2)
                    plt.gca().add_patch(rect)
            # Plottitle
            if not isinstance(title, (list, tuple)) and not titledone:
                plt.title(title)
                titledone = True
            elif title[idx] and i == 0 and not titledone:
                plt.title(title[idx])
            # plt.xlim(x[0],x[-1])

            # labels
            plt.xlabel('Time')
            colname = dat.header.get('col-{}'.format(component), '')
            colunit = dat.header.get('unit-col-{}'.format(component), '')
            if colunit:
                colunit = " [{}]".format(colunit)
            plt.ylabel('{}{}'.format(colname, colunit))
            if labelx:
                labelx = labelx  # axes coords
                ax.yaxis.set_label_coords(labelx, 0.5)

            # Legends
            if legend:
                legenddummy = []
                if separate:
                    legenddummy = [dat.header.get('SensorID', '')]
                if not isinstance(legend, dict):
                    legend = {}
                    if separate:
                        legend["plotnumber"] = i
                    else:
                        legenddummy = [tmpdat.header.get('SensorID', '') for tmpdat in data]
                        legend["plotnumber"] = len(keys[idx]) - 1
                shadow = False
                legendtext = legend.get("legendtext", legenddummy)
                legendposition = legend.get("legendposition", "best")
                legendstyle = legend.get("legendstyle", "shadow")
                if legendstyle == 'shadow':
                    shadow = True
                if legend.get("plotnumber", i) == i:
                    plt.legend(legendtext, loc=legendposition, shadow=shadow)
            # Plot grid
            if grid:
                mygrid = grid
                if not isinstance(mygrid, dict):
                    mygrid = {}
                gridvisible = mygrid.get("visible", True)
                gridwhich = mygrid.get("axis", "major")
                gridaxis = mygrid.get("visible", "both")
                gridcolor = mygrid.get("color", [0.9, 0.9, 0.9])
                plt.grid(visible=gridvisible, which=gridwhich, axis=gridaxis, color=gridcolor)

            total_pos += 1
    return plt


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

    print()
    print("Good-bye!")
    print("----------------------------------------------------------")
