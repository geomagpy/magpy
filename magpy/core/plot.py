
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import *
import matplotlib

gui_env = ['TKAgg', 'GTKAgg', 'Agg'] # remove WXAgg and add this activation to GUI
maversion = matplotlib.__version__.replace('svn', '')
try:
    maversion = map(int, maversion.replace("rc", "").split("."))
    matplotlibversion = list(maversion)
except:
    maversion = maversion.strip("rc")
    matplotlibversion = maversion
logger = logging.getLogger(__name__)
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
from matplotlib.dates import date2num, num2date, DateFormatter
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import matplotlib.patches as patches

edgecolor = [0.8, 0.8, 0.8]


class AutoScaleY():
    # Used to rescale all y axes in a multicomponent plot to optimal range
    # https://stackoverflow.com/questions/53326158/interactive-zoom-with-y-axis-autoscale
    def  __init__(self, line, margin=0.05):
        self.margin = margin
        self.line = line
        self.ax = line.axes
        self.ax.callbacks.connect('xlim_changed', self.rescale_y)

    def rescale_y(self,evt=None):
        xmin, xmax = self.ax.get_xlim()
        x, y = self.line.get_data()
        cond = (x >= xmin) & (x <= xmax)
        yrest = y[cond]
        margin = (yrest.max()-yrest.min())*self.margin
        self.ybounds = [yrest.min()-margin, yrest.max()+margin]
        self.timer = self.ax.figure.canvas.new_timer(interval=10)
        self.timer.single_shot = True
        self.timer.add_callback(self.change_y)
        self.timer.start()

    def change_y(self):
        self.ax.set_ylim(self.ybounds)
        self.ax.figure.canvas.draw()


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


def tsplot(data = None, keys = None, timecolumn = None, xrange = None, yranges = None, padding = None,
    symbols = None, colors = None, title = None, xinds = None, legend = None, grid = None, patch = None, annotate = False,
    fill = None, showpatch = None, errorbars = None, functions = None, functionfmt = "r-", xlabelposition = None,
    ylabelposition = None, yscale = None, dateformatter = None, force = False, width = 10, height = 4, alpha = 0.5,
    variables = None, figure = None, debug=False):
    """
    DESCRIPTION:
        tsplot creates a timeseries plot of selected data. tsplot is highly configureable. fixed contents contain a
        shared x axis based on the first plot.

        This method replaces plotStreams, ploteasy and plot of magpy1.x

    SPEED:
        matplotlib has some speed issues when plotting dates for large datasets as they are internally converted.
        - tsplot uses the following way: time column (datetime or np.datetime64) is converted to matplotlib.dates date2num
              before: this technique is a bot more than twice faster then just plotting dates and rely on internal conversion
        - tsplot needs slightly more then 1 second to display 1 month of 1sec 6 channel data in the backend on my machine

    OPTIONS:
        keys (list)         :    default are the first three av[]ailable keys of the first data set. If only keys for
                                 the first data set are given, then an overlay plot is activated
                                 If all keys are provided then all plots are plotted separately.
        symbols (list)      :    default "-" : define plot symbols - accpeted are "-",".","o" and "k"
        colors (list)       :    default is light grey : defines the color of all individual plots of each datastream
        timecolumn (list)   :    default None : define the used time column - can be time or sectime (if existing)
        yranges (list)      :    default None : define individual ranges for y axes - overrides padding
                                 EXAMPLE: keys=[['x','y'],['f']], yranges=[[[20000,22000],[-1000,1000]],[[44000,54000]]]
        padding (list)      :    default None : define scalar paddings to minimal and maximal data value for y range
                                 EXAMPLE: keys=[['x','y'],['f']], padding=[[100,50],[10]]
        fill (dict)         :    provide a list with specific fill information (dicts) for each key i.e.
                                 [{"boundary":400,"fillcolor":"red"},{"boundary":400,"fillcolor":"blue","fillrange":"smaller"}]
                                 to fill everything above 400 read and everything below 400 blue for key y
                                 EXAMPLE: keys=[['x','y','z']], fill=[[[],[{"boundary":400,"fillcolor":"red"},
                                 {"boundary":400,"fillcolor":"blue","fillrange":"smaller"}],[{"boundary":0,"fillcolor":"red"}]]]
        legend (dict)       :    default None
                                 EXAMPLE: legend={"legendtext":('Primary-LEMI036_2', 'Primary-FGE_S0252'),
                                 "legendposition":"upper right","legendstyle":"shadow","plotnumber":2}
                                 or legend=True for default with SensorID
        grid (dict)         :    default None
                                 EXAMPLE: grid={"visible":True,"which":"major","axis":"both","color":"k"}
                                 or grid=True  for default values
        patch (list/dict)   :    default none - patch contain colored regions covering the full vertical space in each
                                 plot - used for flagging info
                                 {"patch1":{"start":datetime,"end":datetime,"color":color,"alpha":0.2},"patch2":
                                 {"start":datetime,"end":datetime,"color":color,"alpha":0.2}]
        annotate (Bool/dict) :   default False - if True, then annotations are set to start time of patches
        showpatch (list)    :    default True for all streams -
                                 EXAMPLE: data=[stream1,stream2,stream3],showpatches=[True,False,True]
        errorbars (list/dict) :  a list of dicts containing definitions for each plot as follows: i.e. two data stream
                                 with keys=[['x','y'],['var1']]
                                 plot error bars for 'x' and 'var1', errorbars for 'x' are stored in key 'dx',
                                 errorbars for 'var1' are stored in 'var5'
                                 errorbars=[[ex,{}],[evar1]] with:
                                 ex = {'key':'dx','color':'red','marker':'o', 'linestype':'-'}, evar1={'key':'var5'}
                                 if errorbars are selected then general symbols and linestyles will be replaced
        functions (list)     :    a list of functions in the same format as keys, functions will be plotted in functioncolor.
                                 empty values are defined by emtpy lists
                                 EXAMPLE: keys=[['x','y'],['var1']], functions=[[func1,[]],[func2]]
        functionfmt(string) :    Default "r-" for all functions
        xlabelposition (dict) :    default none - if defined then all xlabels are configured as follows
                                 EXAMPLE: not yet implemented
        ylabelposition (float) :    default none - if defined then all ylabels are set to this x position
                                 EXAMPLE: ylabelposition=-0.1
        ysacle (string)     :    'linear' (default), 'log'
        dateformatter (string) : if provided then autoformat x is activated and the choosen format is used for the datecolumn
                                 i.e. dateformatter="%Y-%m-%d %H"
        force (bool)        :    plot even with empty DataStream - can be used to plot flag patches without data
        height (float)      :    default 4 - default height of each individual plot
                                 EXAMPLE: height=2
        width (float)       :    default 10 - default width of all plots
                                 EXAMPLE: width=12
        figure (object)     :    provide a figure object for the plot - used by magpy_gui
                                 EXAMPLE: width=12

    EXAMPLE:
        A) A simple plot in MagPy2.x design
        B) Plot two plots on each other
        Overlay plot of two difference plots with x,y,z respectively
        p = tsplot([delta1,delta2],keys=[['x','y','z']], yranges=[[[-5,5],[-5,5],[-5,5]]], symbolcolor=[[0.8, 0.8, 0.8],[0.5, 0.5, 0.5]], height=2, legend={"legendtext":('Primary-LEMI036_2', 'Primary-FGE_S0252'),"legendposition":"upper right","legendstyle":"shadow","plotnumber":2})
        C) Fill options:
        p = tsplot(ace1,fill=[[[],[{"boundary":400,"fillcolor":"red"},{"boundary":400,"fillcolor":"blue","fillrange":"smaller"}],[{"boundary":0,"fillcolor":"red"}]]])
        D) Plotting errorbars
        e1={'key':'dx','color':'red','capsize':5, 'marker':'o', 'linestyle':'-'}
        e2={'key':'dy'}
        tsplot([teststream,xxx],[['x','y'],['x','y']], symbols=[["-","-"],[".","--"]], symbolcolor=[[0.5, 0.5, 0.5]], errorbars=[[{},{}],[e1,e2]], height=2)

    """
    # This method requires the provision of data sets on which the get_gaps method has been applied
    def is_list_empty(testlist):
        # Replaces older testing method whether array like [[]] are empty
        tester = min(np.array(testlist, dtype=object).shape)
        if tester:
            return False
        else:
            return True

    if not data:
        data = [DataStream()]
    if not keys:
        keys = [['dummy']]
    if not symbols:
        symbols = [[]]
    if not colors:
        colors = [[]]
    if not timecolumn:
        timecolumn = ['time']
    if not yranges:
        yranges = [[]]
    if not yscale:
        yscale = [[]]
    if not padding:
        padding = [[]]
    if not fill:
        fill = [[]]
    if not errorbars:
        errorbars = [[]]
    if not functions:
        functions = [[]]
    if not xinds:
        xinds = [None]
    if not showpatch:
        showpatch = [True]
    if not legend:
        legend = {}
    if not grid:
        grid = {}
    if not patch:
        patch = {}

    titledone = False
    # check if provided data is a list
    if not isinstance(data, (list, tuple)):
        data = [data]
        # amount of plots
    amount = len(data)
    if variables and not keys:
        keys = variables
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
    if is_list_empty(symbols):
        symbols = [["-" for el in line] for line in keys]
    if is_list_empty(colors):
        colors = [[[0.8, 0.8, 0.8] for el in line] for line in keys]
    if is_list_empty(yranges):
        yranges = [[["default", "default"] for el in line] for line in keys]
    if yscale and not isinstance(yscale, (list, tuple)):
        yscale = [[yscale for el in line] for line in keys]
    if is_list_empty(yscale):
        yscale = [["linear" for el in line] for line in keys]
    keys = fill_list(keys, amount, keys[-1])
    symbols = fill_list(symbols, amount, symbols[-1])
    colors = fill_list(colors, amount, colors[-1])
    yscale = fill_list(yscale, amount, yscale[-1])
    yranges = fill_list(yranges, amount, yranges[-1])
    timecolumn = fill_list(timecolumn, amount, timecolumn[-1])
    showpatch = fill_list(showpatch, amount, showpatch[-1])
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
    if not is_list_empty(padding):
        if len(padding[0]) == len(keys[0]):
            padding = fill_list(padding, amount, padding[-1])
        else:
            padding = None
    skey = list(np.array(keys, dtype=object).shape)[:2]
    if not is_list_empty(errorbars):
        if not isinstance(errorbars, (list, tuple)):
            errorbars = False
        elif not skey == list(np.array(errorbars, dtype=object).shape)[:2]:
            print("Given error bars do not fit in shape to keys - skipping")
            errorbars = False
    if not is_list_empty(functions):
        if not isinstance(functions, (list, tuple)):
            functions = False
        elif not skey == list(np.array(functions, dtype=object).shape)[:2]:
            print("Given functions do not fit in shape to keys - skipping")
            functions = False
    if not functionfmt:
        functionfmt = 'r-'

    if not figure:
        fig = plt.figure(figsize=(width, hght))
    else:
        fig = figure

    # parameter for separate plots
    total_pos = 0
    total_keys = np.concatenate(keys).size
    annocount = 0
    yoff = -10
    axs=[]

    if debug:
        t1 = datetime.now()
    for idx, dat in enumerate(data):
        # x column
        if debug:
            t2 = datetime.now()
        x = dat._get_column(timecolumn[idx])
        if len(x) > 0 and testtimestep(x[0]):
            # Testdata set: if datetime is converted here total plotting needs 0.25 sec
            # Testdata set: if datetime is used here as is total plotting needs 0.60 sec
            t = date2num(x)  # for sharex numerical dates are apparently necessary
            # 0. date directly (~0 sec), but in later stage much longer (remove ax.axis_date when testing)
        else:
            t = x
        if not xinds[idx]:
            # plot only selected indicies
            xinds[idx] = list(range(0, len(x), 1))
        # drop nan columns
        dat = dat._remove_nancolumns()
        if debug:
            t3 = datetime.now()
        for i, component in enumerate(keys[idx]):
            comp = dat._get_column(component)
            if len(comp) > 0 or force:
                if separate:
                    subplot = int("{}1{}".format(total_keys, total_pos + 1))
                else:
                    subplot = int("{}1{}".format(len(keys[idx]), i + 1))
                ax = plt.subplot(subplot)
                axs.append(ax)
                if testtimestep(x[0]):
                    ax.xaxis.axis_date()
                # Symbols and color
                # ------------------
                symbol = symbols[idx][i]
                try:
                    color = colors[idx][i]
                except:
                    color = [0.8, 0.8, 0.8]
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
                    color = None
                    yranges[idx][i] = [0, 9]
                # Fill between graphs
                # ------------------
                if fill and not is_list_empty(fill):
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
                            ax.fill_between(t, comp, boundary, color=fillcolor, alpha=fillalpha, where=indi)
                # Error bars
                # ------------------
                if errorbars and not is_list_empty(errorbars):
                    errordict = errorbars[idx][i]
                    if errordict and isinstance(errordict, dict):
                        yerr = dat._get_column(errordict.get('key'))
                        if len(yerr) > 0:
                            errorcolor = errordict.get('color', color)
                            capsize = errordict.get('capsize', 3)
                            marker = errordict.get('marker', '.')
                            linestyle = errordict.get('linestyle', '-')
                            plt.errorbar(t, comp, yerr=yerr, linestyle=linestyle, marker=marker, color=color,
                                         capsize=capsize, ecolor=errorcolor)
                # Plot the main graph
                # ------------------
                if force and not len(comp) > 0 and patch and not is_list_empty(patch):
                    # need the time range covered by patches
                    line, = ax.plot(t, [0.5] * len(t), 'w-', alpha=0.0)
                    r = AutoScaleY(line)
                    mincomp = 0
                    maxcomp = 1
                else:
                    line, = ax.plot(t, comp, symbol, color=color)
                    r = AutoScaleY(line)
                    mincomp = np.nanmin(comp)
                    maxcomp = np.nanmax(comp)
                # plt.hlines(0,t[0],t[-1])
                plt.yscale(yscale[idx][i])
                if dateformatter:
                    plt.gca().xaxis.set_major_formatter(DateFormatter(dateformatter))
                    # plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))
                    plt.gcf().autofmt_xdate()
                # Padding and y ranges
                # ------------------
                adjustrange = False
                if yranges and not is_list_empty(yranges):
                    adjustrange = False
                    if not yranges[idx][i][0] == "default":
                        adjustrange = True
                        mincomp = yranges[idx][i][0]
                    if not yranges[idx][i][1] == "default":
                        adjustrange = True
                        maxcomp = yranges[idx][i][1]
                    if adjustrange:
                        plt.ylim(mincomp, maxcomp)
                if padding and not is_list_empty(padding) and not adjustrange:
                    mincomp = mincomp - padding[idx][i]
                    maxcomp = maxcomp + padding[idx][i]
                    plt.ylim(mincomp, maxcomp)
                # Plot patches - for flagging etc
                # ------------------
                if isinstance(patch, list):
                    pat = patch[idx][i]
                else:
                    pat = patch
                if pat and isinstance(pat, dict) and showpatch[idx]:
                    for line in pat:
                        l = pat.get(line)
                        patchcomps = l.get('components')
                        if component in patchcomps:
                            # winmin = date2num(l.get('start'))
                            # winmax = date2num(l.get('end'))
                            winmin = l.get('start')
                            winmax = l.get('end')
                            edgecolor = l.get('color')
                            if not edgecolor:
                                if l.get('flag', 0) in [1, 3]:
                                    edgecolor = 'r'
                                elif l.get('flag', 0) in [2, 4]:
                                    edgecolor = 'g'
                                elif l.get('flag', 0) == 0:
                                    edgecolor = 'y'
                            rect = patches.Rectangle((winmin, mincomp), winmax - winmin, maxcomp - mincomp,
                                                     edgecolor=edgecolor, facecolor=edgecolor, alpha=0.2)
                            plt.gca().add_patch(rect)
                            # Annotations
                            # ------------------
                            if annotate:
                                annosign = -1
                                annocount += 1
                                annotext = line
                                textx = 0
                                texty = 0
                                if annotate == "label":
                                    annotext = l.get('label')
                                elif annotate == "labelid":
                                    annotext = l.get('labelid')
                                    #annosign = 1
                                    #texty = rect.get_y()
                                if annocount % 2:
                                    yoff = 20
                                else:
                                    yoff = 10
                                plt.gca().annotate(annotext, (rect.get_x() + rect.get_width() / 2, maxcomp), xytext = (textx, texty + annosign*yoff),
                                                   textcoords = 'offset points', ha = 'center', va = 'bottom')



                # Plottitle
                # ------------------
                if not isinstance(title, (list, tuple)) and not titledone:
                    plt.title(title)
                    titledone = True
                elif title[idx] and i == 0 and not titledone:
                    plt.title(title[idx])
                # plt.xlim(x[0],x[-1])
                # Functions
                # ------------------
                if functions and not is_list_empty(functions):
                    function = functions[idx][i]
                    if function and isinstance(function, (list, tuple)):
                        if len(np.array(function,
                                        dtype=object).shape) > 1:  # allow multiple functions for each component
                            for functio in function:
                                print (functio)
                                # function should contain the fitted time range and the projected timerange
                                fres = evaluate_function(component, functio, dat.samplingrate(), starttime=None,
                                                         endtime=None, debug=False)
                                if fres and len(fres) == 2:
                                    ax.plot(fres[0], fres[1], functionfmt, alpha=0.5)
                        else:
                            print (function)
                            # function should contain the fitted time range and the projected timerange
                            fres = evaluate_function(component, function, dat.samplingrate(), starttime=None,
                                                     endtime=None, debug=False)
                            if fres and len(fres) == 2:
                                ax.plot(fres[0], fres[1], functionfmt, alpha=0.5)
                # Labels
                # ------------------
                plt.xlabel('Time')
                colname = dat.header.get('col-{}'.format(component), '')
                colunit = dat.header.get('unit-col-{}'.format(component), '')
                if colunit:
                    colunit = " [{}]".format(colunit)
                plt.ylabel('{}{}'.format(colname, colunit))
                if ylabelposition:
                    ylabelposition = ylabelposition  # axes coords
                    ax.yaxis.set_label_coords(ylabelposition, 0.5)
                # TODO xlabel positions
                # Legends
                # ------------------
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
                # ------------------
                if grid:
                    mygrid = grid
                    if not isinstance(mygrid, dict):
                        mygrid = {}
                    gridvisible = mygrid.get("visible", True)
                    gridwhich = mygrid.get("which", "major")
                    gridaxis = mygrid.get("axis", "both")
                    gridcolor = mygrid.get("color", [0.9, 0.9, 0.9])
                    plt.grid(visible=gridvisible, which=gridwhich, axis=gridaxis, color=gridcolor)
                # Remove offsets on y-axes
                try:
                    # Might fail for various reasons - just ignore
                    plt.ticklabel_format(useOffset=False, style='plain', axis='y')
                except:
                    pass
                if i > 0:
                    ax.sharex(axs[0])
                #plt.tight_layout()

                total_pos += 1
            else:
                print(" tsplot: warning component {} of stream {} is empty".format(component, idx))
    if debug:
        t4 = datetime.now()
        print ("TIMING total:", (t4-t1).total_seconds())
        print ("TIMING time conversion:", (t3-t2).total_seconds())

    return fig, plt.gca()



#####################################################################
#                                                                   #
#       TESTING                                                     #
#       Run this after making changes:                              #
#       $ python3 plot.py                                          #
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
        array[KEYLIST.index('sectime')] = np.asarray(
            [startdate + timedelta(minutes=i) for i in range(0, len(array[1]))]) + timedelta(minutes=15)
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
        return teststream

    teststream = create_minteststream()
    errors = {}
    try:
        v1 = datetime.now(timezone.utc).replace(tzinfo=None)
        v2 = np.datetime64(v1)
        v3 = date2num(v1)
        # can also be used for unittest
        var1 = testtimestep(v1)
        var2 = testtimestep(v2)
        var3 = testtimestep(v3)
    except Exception as excep:
        errors['testtimestep'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR testing number.")
    try:
        ml = [1,2,3,4,5,6,7,8,9]
        v1 = fill_list(ml, 19, 10)
        # can also be used for unittest with np.sum
        #print (np.sum(v1), np.sum(ml)) # +100 for unittest
    except Exception as excep:
        errors['fill_list'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR testing number.")
    try:
        fig,ax = tsplot([teststream])
        pass
    except Exception as excep:
        errors['tsplot'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR testing number.")

    print()
    print("----------------------------------------------------------")
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(str(errors.keys()))
        print()
        print("Exceptions thrown:")
        for item in errors:
            print("{} : errormessage = {}".format(item, errors.get(item)))
