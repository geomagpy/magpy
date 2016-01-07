#!/usr/bin/env python

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

try: # Necessary for wx2.8.11.0
    from wx.lib.pubsub import setupkwargs
except:
    pass
from wx.lib.pubsub import pub

try:
    from stream import *
    from absolutes import *
    from transfer import *
    from database import *
    from version import __version__
    from gui.streampage import *
    from gui.dialogclasses import *
    from gui.absolutespage import *
    from gui.developpage import *
    from gui.analysispage import *
except:
    from magpy.stream import *
    from magpy.absolutes import *
    from magpy.transfer import *
    from magpy.database import *
    from magpy.version import __version__
    from magpy.gui.streampage import *
    from magpy.gui.dialogclasses import *
    from magpy.gui.absolutespage import *
    from magpy.gui.developpage import *
    from magpy.gui.analysispage import *

import glob, os, pickle, base64

def saveobj(obj, filename):
    with open(filename, 'wb') as f:
        pickle.dump(obj,f,pickle.HIGHEST_PROTOCOL)

def loadobj(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)

def saveini(dbname=None, user=None, passwd=None, host=None, filename=None, dirname=None, compselect=None, abscompselect=None, basecompselect=None, resolution=None, dipathlist = None, divariopath = None, discalarpath = None, diexpD = None, diexpI = None, stationid = None, diid = None, ditype = None, diazimuth = None, dipier = None, dialpha = None, dideltaF = None, didbadd = None):
    """
    Method for creating credentials
    """

    if not dbname:
        dbname = 'None'
    if not user:
        user = 'max'
    if not passwd:
        passwd = 'secret'
    if not host:
        host = 'localhost'
    if not filename:
        filename = 'noname.txt'
    if not dirname:
        dirname = '/srv'
    if not resolution:
        resolution = 10000
    if not compselect:
        compselect = 'xyz'

    if not abscompselect:
        abscompselect = 'xyz'
    if not basecompselect:
        basecompselect = 'bspline'

    if not dipathlist:
        dipathlist = ''
    if not divariopath:
        divariopath = ''
    if not discalarpath:
        discalarpath = ''
    if not diexpD:
        diexpD = 3.0
    if not diexpI:
        diexpI = 64.0
    if not stationid:
        stationid = 'wic'
    if not diid:
        diid = ''
    if not ditype:
        ditype = 'DI' #abstype = ''
    if not diazimuth:
        diazimuth = ''
    if not dipier:
        dipier = 'A2'
    if not dialpha:
        dialpha = '0.0'
    if not dideltaF:
        dideltaF = '2.1'
    if not didbadd:
        didbadd = False
    # starttime,endtime
    #if not db=db # test whether db is connected

    home = os.path.expanduser('~')
    initpath = os.path.join(home,'.magpyguiini')

    pwd = base64.b64encode(passwd)

    dictionary = {'dbname': dbname, 'user':user, 'passwd':pwd, 'host':host, \
                  'filename': filename, 'dirname':dirname, \
                  'compselect':compselect, 'abscompselect':abscompselect, \
                  'basecompselect':basecompselect, 'resolution':resolution, 'dipathlist':dipathlist, 'divariopath':divariopath, 'discalarpath':discalarpath, 'diexpD':diexpD, 'diexpI':diexpI, 'stationid':stationid, 'diid':diid, 'ditype':ditype, 'diazimuth':diazimuth, 'dipier':dipier, 'dialpha':dialpha, 'dideltaF':dideltaF, 'didbadd':didbadd }

    saveobj(dictionary, initpath)
    #print "Initialization: Added data "


def loadini():
    """
    Load initialisation data

    """
    home = os.path.expanduser('~')
    initpath = os.path.join(home,'.magpyguiini')
    #print "Trying to access initialization file:", initpath

    try:
        initdata = loadobj(initpath)
    except:
        #print "Init file not found: Could not load data"
        return False

    #print "Initialization data loaded"
    return initdata


class RedirectText(object):
    # Taken from: http://www.blog.pythonlibrary.org/2009/01/01/wxpython-redirecting-stdout-stderr/
    # Used to redirect di results to the multiline textctrl on the DI page
    def __init__(self,aWxTextCtrl):
        self.out=aWxTextCtrl

    def write(self,string):
        self.out.WriteText(string)

class PlotPanel(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.figure = plt.figure()
        scsetmp = ScreenSelections()
        self.canvas = FigureCanvas(self,-1,self.figure)
        self.initialPlot()
        self.__do_layout()

    def __do_layout(self):
        # Resize graph and toolbar, create toolbar
        self.vbox = wx.BoxSizer(wx.VERTICAL)
        self.vbox.Add(self.canvas, 1, wx.LEFT | wx.TOP | wx.GROW)
        self.toolbar = NavigationToolbar2Wx(self.canvas)
        self.vbox.Add(self.toolbar, 0, wx.EXPAND)
        self.SetSizer(self.vbox)
        self.vbox.Fit(self)

    def guiPlot(self,stream,keys,**kwargs):
        """
        DEFINITION:
            embbed matplotlib figure in canvas

        PARAMETERS:
        kwargs:  - all plot args
        """
        self.figure.clear()
        try:
            self.axes.clear()
        except:
            pass
        self.axes = stream.plot(keys,figure=self.figure,**kwargs)
        t = [elem.time for elem in stream]
        flag = [elem.flag for elem in stream]
        k = [eval("elem."+keys[0]) for elem in stream]
        #self.axes.af2 = self.AnnoteFinder(t,yplt,flag,self.axes)
        self.axes.af2 = self.AnnoteFinder(t,k,flag,self.axes)
        self.figure.canvas.mpl_connect('button_press_event', self.axes.af2)
        self.canvas.draw()

    def initialPlot(self):
        """
        DEFINITION:
            loads an image for the startup screen
        """
        try:
            self.axes = self.figure.add_subplot(111)
            plt.axis("off") # turn off axis
            startupimage = 'magpy.png'
            # TODO add alternative positions
            # either use a walk to locate the image in /usr for linux and installation path on win
            # or put installation path in ini
            img = imread(startupimage)
            self.axes.imshow(img)
            self.canvas.draw()
        except:
            pass

    def linkRep(self):
        return ReportPage(self)

    class AnnoteFinder:
        """
        callback for matplotlib to display an annotation when points are clicked on.  The
        point which is closest to the click and within xtol and ytol is identified.

        Register this function like this:

        scatter(xdata, ydata)
        af = AnnoteFinder(xdata, ydata, annotes)
        connect('button_press_event', af)
        """

        def __init__(self, xdata, ydata, annotes, axis=None, xtol=None, ytol=None):
            self.data = zip(xdata, ydata, annotes)
            if xtol is None:
                xtol = ((max(xdata) - min(xdata))/float(len(xdata)))/2
            if ytol is None:
                ytol = ((max(ydata) - min(ydata))/float(len(ydata)))/2
            ymin = min(ydata)
            ymax = max(ydata)
            self.xtol = xtol
            self.ytol = ytol
            if axis is None:
                self.axis = pylab.gca()
            else:
                self.axis= axis
            self.drawnAnnotations = {}
            self.links = []

        def distance(self, x1, x2, y1, y2):
            """
            return the distance between two points
            """
            return math.hypot(x1 - x2, y1 - y2)

        def __call__(self, event):
            if event.inaxes:
                clickX = event.xdata
                clickY = event.ydata
                if self.axis is None or self.axis==event.inaxes:
                    annotes = []
                    for x,y,a in self.data:
                        #if  clickX-self.xtol < x < clickX+self.xtol and clickY-self.ytol < y < clickY+self.ytol:
                        if  clickX-self.xtol < x < clickX+self.xtol :
                            annotes.append((self.distance(x,clickX,y,clickY),x,y, a) )
                    if annotes:
                        annotes.sort()
                        distance, x, y, annote = annotes[0]
                        self.drawAnnote(event.inaxes, x, y, annote)
                        for l in self.links:
                            l.drawSpecificAnnote(annote)

        def drawAnnote(self, axis, x, y, annote):
            """
            Draw the annotation on the plot
            """
            if (x,y) in self.drawnAnnotations:
                markers = self.drawnAnnotations[(x,y)]
                for m in markers:
                    m.set_visible(not m.get_visible())
                self.axis.figure.canvas.draw()
            else:
                #t = axis.text(x,y, "(%3.2f, %3.2f) - %s"%(x,y,annote), )
                datum = datetime.strftime(num2date(x).replace(tzinfo=None),"%Y-%m-%d")
                t = axis.text(x,y, "(%s, %3.2f)"%(datum,y), )
                m = axis.scatter([x],[y], marker='d', c='r', zorder=100)
                scse = ScreenSelections()
                scse.seldatelist.append(x)
                scse.selvallist.append(y)
                scse.updateList()
                #test = MainFrame(parent=None)
                #test.ReportPage.addMsg(str(x))
                #rep_page.logMsg('Datum is %s ' % (datum))
                #l = axis.plot([x,x],[0,y])
                self.drawnAnnotations[(x,y)] =(t,m)
                self.axis.figure.canvas.draw()

        def drawSpecificAnnote(self, annote):
            annotesToDraw = [(x,y,a) for x,y,a in self.data if a==annote]
            for x,y,a in annotesToDraw:
                self.drawAnnote(self.axis, x, y, a)


class MenuPanel(wx.Panel):
    #def __init__(self, parent):
    #    wx.Panel.__init__(self,parent,-1,size=(100,100))
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        # Create pages on MenuPanel
        nb = wx.Notebook(self,-1)
        self.str_page = StreamPage(nb)
        self.ana_page = AnalysisPage(nb)
        self.abs_page = AbsolutePage(nb)
        self.rep_page = ReportPage(nb)
        self.com_page = PortCommunicationPage(nb)
        nb.AddPage(self.str_page, "Stream")
        nb.AddPage(self.ana_page, "Analysis")
        nb.AddPage(self.abs_page, "DI")
        nb.AddPage(self.rep_page, "Report")
        nb.AddPage(self.com_page, "Monitor")

        sizer = wx.BoxSizer()
        sizer.Add(nb, 1, wx.EXPAND)
        self.SetSizer(sizer)


class MainFrame(wx.Frame):
    def __init__(self, *args, **kwds):
        kwds["style"] = wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, *args, **kwds)
        # The Splitted Window
        self.sp = wx.SplitterWindow(self, -1, style=wx.SP_3D|wx.SP_BORDER)
        self.plot_p = PlotPanel(self.sp,-1)
        self.menu_p = MenuPanel(self.sp,-1)
        pub.subscribe(self.changeStatusbar, 'changeStatusbar')

        # The Status Bar
        self.StatusBar = self.CreateStatusBar(2, wx.ST_SIZEGRIP)
        #self.changeStatusbar("Ready")

        self.stream = DataStream() # used for storing original data
        self.plotstream = DataStream() # used for manipulated data
        self.shownkeylist = []
        self.keylist = []
        self.compselect = 'None'
        self.dipathlist = []
        self.divariopath = ''
        self.discalarpath = ''


        # Try to load ini-file
        # located within home directory
        inipara = loadini()
        if not inipara:
            saveini() # initialize defaultvalues
            inipara = loadini()

        # Variable initializations
        self.initParameter(inipara)

        # Menu Bar
        # --------------
        self.MainMenu = wx.MenuBar()
        # ## File Menu
        self.FileMenu = wx.Menu()
        self.FileOpen = wx.MenuItem(self.FileMenu, 101, "&Open File...\tCtrl+O", "Open file", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.FileOpen)
        self.DirOpen = wx.MenuItem(self.FileMenu, 102, "Select &Directory...\tCtrl+D", "Select an existing directory", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.DirOpen)
        self.WebOpen = wx.MenuItem(self.FileMenu, 103, "Open &URL...\tCtrl+U", "Get data from the internet", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.WebOpen)
        self.DBOpen = wx.MenuItem(self.FileMenu, 104, "&Select DB table...\tCtrl+S", "Select a MySQL database", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.DBOpen)
        self.DBOpen.Enable(False)
        self.FileMenu.AppendSeparator()
        self.ExportData = wx.MenuItem(self.FileMenu, 105, "&Export data...\tCtrl+E", "Export data to a file", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.ExportData)
        self.ExportData.Enable(False)
        self.FileMenu.AppendSeparator()
        self.FileQuitItem = wx.MenuItem(self.FileMenu, wx.ID_EXIT, "&Quit\tCtrl+Q", "Quit the program", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.FileQuitItem)
        self.MainMenu.Append(self.FileMenu, "&File")
        # ## Database Menu
        self.DatabaseMenu = wx.Menu()
        self.DBConnect = wx.MenuItem(self.DatabaseMenu, 201, "&Connect MySQL DB...\tCtrl+M", "Connect Database", wx.ITEM_NORMAL)
        self.DatabaseMenu.AppendItem(self.DBConnect)
        self.MainMenu.Append(self.DatabaseMenu, "Data&base")
        # ## DI Menu
        self.DIMenu = wx.Menu()
        self.DIPath2DI = wx.MenuItem(self.DIMenu, 501, "Load DI data...\tCtrl+L", "Load DI data...", wx.ITEM_NORMAL)
        self.DIMenu.AppendItem(self.DIPath2DI)
        self.DIPath2Vario = wx.MenuItem(self.DIMenu, 502, "Path to variometer data...\tCtrl+A", "Variometer data...", wx.ITEM_NORMAL)
        self.DIMenu.AppendItem(self.DIPath2Vario)
        self.DIPath2Scalar = wx.MenuItem(self.DIMenu, 503, "Path to scalar data...\tCtrl+R", "Scalar data...", wx.ITEM_NORMAL)
        self.DIMenu.AppendItem(self.DIPath2Scalar)
        self.MainMenu.Append(self.DIMenu, "D&I")
        # ## Options Menu
        self.OptionsMenu = wx.Menu()
        self.OptionsInitItem = wx.MenuItem(self.OptionsMenu, 401, "&Initialisation/Calculation parameter\tCtrl+I", "Modify initialisation/calculation parameters (e.g. filters, sensitivity)", wx.ITEM_NORMAL)
        self.OptionsMenu.AppendItem(self.OptionsInitItem)
        self.OptionsMenu.AppendSeparator()
        self.OptionsObsItem = wx.MenuItem(self.OptionsMenu, 402, "&Observatory specifications\tCtrl+O", "Modify observatory specific meta data (e.g. pears, offsets)", wx.ITEM_NORMAL)
        self.OptionsMenu.AppendItem(self.OptionsObsItem)
        self.MainMenu.Append(self.OptionsMenu, "&Options")
        self.HelpMenu = wx.Menu()
        self.HelpAboutItem = wx.MenuItem(self.HelpMenu, 301, "&About...", "Display general information about the program", wx.ITEM_NORMAL)
        self.HelpMenu.AppendItem(self.HelpAboutItem)
        self.MainMenu.Append(self.HelpMenu, "&Help")
        self.SetMenuBar(self.MainMenu)
        # Menu Bar end


        self.__set_properties()

        # BindingControls on the menu
        self.Bind(wx.EVT_MENU, self.OnOpenDir, self.DirOpen)
        self.Bind(wx.EVT_MENU, self.OnOpenFile, self.FileOpen)
        self.Bind(wx.EVT_MENU, self.OnOpenURL, self.WebOpen)
        self.Bind(wx.EVT_MENU, self.OnOpenDB, self.DBOpen)
        self.Bind(wx.EVT_MENU, self.OnExportData, self.ExportData)
        self.Bind(wx.EVT_MENU, self.OnFileQuit, self.FileQuitItem)
        self.Bind(wx.EVT_MENU, self.OnDBConnect, self.DBConnect)
        self.Bind(wx.EVT_MENU, self.onLoadDI, self.DIPath2DI)
        self.Bind(wx.EVT_MENU, self.onDefineVario, self.DIPath2Vario)
        self.Bind(wx.EVT_MENU, self.onDefineScalar, self.DIPath2Scalar)
        self.Bind(wx.EVT_MENU, self.OnOptionsInit, self.OptionsInitItem)
        self.Bind(wx.EVT_MENU, self.OnOptionsObs, self.OptionsObsItem)
        self.Bind(wx.EVT_MENU, self.OnHelpAbout, self.HelpAboutItem)
        # BindingControls on the notebooks
        #       Stream Page
        # ------------------------
        self.Bind(wx.EVT_BUTTON, self.onOpenStreamButton, self.menu_p.str_page.openStreamButton)
        self.Bind(wx.EVT_BUTTON, self.onReDrawButton, self.menu_p.str_page.DrawButton)
        self.Bind(wx.EVT_BUTTON, self.onSelectKeys, self.menu_p.str_page.selectKeysButton)
        self.Bind(wx.EVT_BUTTON, self.onExtractData, self.menu_p.str_page.extractValuesButton)
        self.Bind(wx.EVT_BUTTON, self.onChangePlotOptions, self.menu_p.str_page.changePlotButton)
        self.Bind(wx.EVT_BUTTON, self.onRestoreData, self.menu_p.str_page.restoreButton)
        self.Bind(wx.EVT_RADIOBOX, self.onChangeComp, self.menu_p.str_page.compRadioBox)
        #        Analysis Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.onFilterButton, self.menu_p.ana_page.filterButton)
        self.Bind(wx.EVT_BUTTON, self.onRemoveOutlierButton, self.menu_p.ana_page.outlierButton)
        self.Bind(wx.EVT_BUTTON, self.onDerivativeButton, self.menu_p.ana_page.derivativeButton)
        self.Bind(wx.EVT_BUTTON, self.onFitButton, self.menu_p.ana_page.fitButton)
        self.Bind(wx.EVT_BUTTON, self.onOffsetButton, self.menu_p.ana_page.offsetButton)
        self.Bind(wx.EVT_BUTTON, self.onActivityButton, self.menu_p.ana_page.activityButton)
        self.Bind(wx.EVT_BUTTON, self.onRotationButton, self.menu_p.ana_page.rotationButton)
        #        DI Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.onLoadDI, self.menu_p.abs_page.loadDIButton)
        self.Bind(wx.EVT_BUTTON, self.onDefineVario, self.menu_p.abs_page.defineVarioButton)
        self.Bind(wx.EVT_BUTTON, self.onDefineScalar, self.menu_p.abs_page.defineScalarButton)
        self.Bind(wx.EVT_BUTTON, self.onDIAnalyze, self.menu_p.abs_page.AnalyzeButton)
        self.Bind(wx.EVT_BUTTON, self.onDISetParameter, self.menu_p.abs_page.advancedButton)

        #self.Bind(wx.EVT_CUSTOM_NAME, self.addMsg)
        # Put something on Report page
        self.menu_p.rep_page.logMsg('Begin logging...')
        # Eventually kill this redirection because it might cause problems from other classes
        #redir=RedirectText(self.menu_p.rep_page.logMsg) # Start redirecting stdout to log window
        #sys.stdout=redir

        # Connect to database
        self._db_connect(self.host, self.user, self.passwd, self.dbname)


        # Disable yet unavailbale buttons
        # --------------------------
        self.menu_p.abs_page.AnalyzeButton.Disable()

        self.menu_p.ana_page.activityButton.Disable() # enabled at initial plot
        self.menu_p.ana_page.offsetButton.Disable()
        self.menu_p.ana_page.fitButton.Disable()
        self.menu_p.ana_page.filterButton.Disable()
        self.menu_p.ana_page.outlierButton.Disable()
        self.menu_p.ana_page.derivativeButton.Disable()
        self.menu_p.ana_page.rotationButton.Disable()

        self.sp.SplitVertically(self.plot_p,self.menu_p,700)

    def __set_properties(self):
        self.SetTitle("MagPy")
        self.SetSize((1100, 700))
        self.SetFocus()
        self.StatusBar.SetStatusWidths([-1, -1])
        # statusbar fields
        StatusBar_fields = ["Ready", ""]
        for i in range(len(StatusBar_fields)):
            self.StatusBar.SetStatusText(StatusBar_fields[i], i)
        self.menu_p.SetMinSize((100, 100))
        self.plot_p.SetMinSize((100, 100))


    def initParameter(self, dictionary):
        # Variable initializations
        self.dbname = dictionary['dbname']
        self.user = dictionary['user']
        pwd = dictionary['passwd']
        self.passwd = base64.b64decode(pwd)
        self.host = dictionary['host']
        self.filename = dictionary['filename']
        self.dirname = dictionary['dirname']
        self.resolution = dictionary['resolution']
        self.compselect = dictionary['compselect']

        self.abscompselect = dictionary['abscompselect']
        self.bascompselect = dictionary['basecompselect']

        self.dipathlist = dictionary['dipathlist']
        self.divariopath = dictionary['divariopath']
        self.discalarpath = dictionary['discalarpath']
        self.diexpD = dictionary['diexpD']
        self.diexpI = dictionary['diexpI']
        self.stationid = dictionary['stationid']
        self.diid = dictionary['diid']
        self.ditype = dictionary['ditype'] #abstype
        self.diazimuth = dictionary['diazimuth']
        self.dipier = dictionary['dipier']
        self.dialpha = dictionary['dialpha']
        self.dideltaF = dictionary['dideltaF']
        self.didbadd = dictionary['didbadd']

    # ################
    # Helper methods:

    def defaultFileDialogOptions(self):
        ''' Return a dictionary with file dialog options that can be
            used in both the save file dialog as well as in the open
            file dialog. '''
        return dict(message='Choose a file', defaultDir=self.dirname,
                    wildcard='*.*')

    def askUserForFilename(self, **dialogOptions):
        dialog = wx.FileDialog(self, **dialogOptions)
        if dialog.ShowModal() == wx.ID_OK:
            userProvidedFilename = True
            self.filename = dialog.GetFilename()
            self.dirname = dialog.GetDirectory()
            #self.SetTitle() # Update the window title with the new filename
        else:
            userProvidedFilename = False
        dialog.Destroy()
        return userProvidedFilename


    def OnInitialPlot(self, stream):
        """
        DEFINITION:
            read stream, extract columns with values and display up to three of them by defailt
            executes guiPlot then
        """
        self.changeStatusbar("Plotting...")
        keylist = []
        keylist = stream._get_key_headers(limit=9)
        # TODO: remove keys with high percentage of nans
        #for key in keylist:
        #    ar = [eval('elem.'+key) for elem in stream if not isnan(eval('elem.'+key))]
        #    div = float(len(ar))/float(len(stream))*100.0
        #    if div <= 5.:
        #        keylist.remove(key)
        self.shownkeylist = keylist

        # check comp
        try:
            self.compselect = stream[0].typ[:3]
            self.menu_p.str_page.compRadioBox.Enable()
            self.menu_p.str_page.compRadioBox.SetStringSelection(self.compselect)
        except:
            if 'x' in keylist and 'y' in keylist and 'z' in keylist:
                self.compselect = 'xyz'
                self.menu_p.str_page.compRadioBox.Enable()

        self.menu_p.rep_page.logMsg('- keys: %s' % (', '.join(keylist)))
        if len(stream) > self.resolution:
            self.menu_p.rep_page.logMsg('- warning: resolution of plot reduced by a factor of %i' % (int(len(stream)/self.resolution)))

        self.plot_p.guiPlot(stream,keylist,resolution=self.resolution)
        if len(stream) > 0 and len(keylist) > 0:
            self.ExportData.Enable(True)
            self.menu_p.ana_page.activityButton.Enable() # enabled at initial plot
            self.menu_p.ana_page.offsetButton.Enable()
            self.menu_p.ana_page.fitButton.Enable()
            self.menu_p.ana_page.activityButton.Enable()
            self.menu_p.ana_page.filterButton.Enable()
            self.menu_p.ana_page.outlierButton.Enable()
            self.menu_p.ana_page.derivativeButton.Enable()
            self.menu_p.ana_page.rotationButton.Enable()

        self.changeStatusbar("Ready")

    def OnPlot(self, stream, keylist, **kwargs):
        """
        DEFINITION:
            read stream and display
        """
        self.changeStatusbar("Plotting...")
        self.plot_p.guiPlot(stream,keylist,resolution=self.resolution,**kwargs)
        if len(stream) > 0 and len(keylist) > 0:
            self.ExportData.Enable(True)
        self.changeStatusbar("Ready")


    # ################
    # Top menu methods:


    def OnHelpAbout(self, event):

        description = """MagPy is developed for geomagnetic analysis.
Features include a support of many data formats, visualization,
advanced anaylsis routines, url/database accessability, DI analysis,
non-geomagnetic data support and more.
"""

        licence = """MagPy is free software; you can redistribute
it and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 2 of the License,
or any later version.

MagPy is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details. You should have
received a copy of the GNU General Public License along with MagPy;
if not, write to the Free Software Foundation, Inc., 59 Temple Place,
Suite 330, Boston, MA  02111-1307  USA"""


        info = wx.AboutDialogInfo()

        info.SetIcon(wx.Icon('magpy128.xpm', wx.BITMAP_TYPE_XPM))
        info.SetName('MagPy')
        info.SetVersion(__version__)
        info.SetDescription(description)
        info.SetCopyright('(C) 2011 - 2016 Roman Leonhardt')
        info.SetWebSite('http://www.conrad-observatory.at')
        info.SetLicence(licence)
        info.AddDeveloper('Roman Leonhardt, Rachel Bailey')
        info.AddDocWriter('Leonhardt,Bailey')
        info.AddArtist('Leonhardt')
        info.AddTranslator('Bailey')

        wx.AboutBox(info)



    def OnExit(self, event):
        self.Close()  # Close the main window.

    def ReactivateStreamPage(self):
            self.menu_p.str_page.fileTextCtrl.Enable()
            self.menu_p.str_page.pathTextCtrl.Enable()
            self.menu_p.str_page.startDatePicker.Enable()
            self.menu_p.str_page.endDatePicker.Enable()
            self.menu_p.str_page.startTimePicker.Enable()
            self.menu_p.str_page.endTimePicker.Enable()
            self.menu_p.str_page.openStreamButton.Enable()


    def SetPageValues(self, stream):
        """
        Method to update all pages with the streams values
        """
        # Length
        n = len(stream)
        # keys
        keys = stream._get_key_headers()
        keystr = ','.join(keys)
        # Sampling rate
        sr = stream.get_sampling_period()*24*3600
        if (sr < 1):
            sr = np.round(sr,3)
        elif (1 <= sr < 10):
            sr = np.round(sr,1)
        else:
            sr = np.round(sr,0)
        # Coverage
        mintime = stream._get_min('time')
        maxtime = stream._get_max('time')

        self.menu_p.ana_page.amountTextCtrl.SetValue(str(n))
        self.menu_p.ana_page.samplingrateTextCtrl.SetValue(str(sr))
        self.menu_p.ana_page.keysTextCtrl.SetValue(keystr)
        print wx.DateTimeFromTimeT(time.mktime(num2date(mintime).timetuple()))
        self.menu_p.str_page.startDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(num2date(mintime).timetuple())))
        self.menu_p.str_page.endDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(num2date(maxtime).timetuple())))
        self.menu_p.str_page.startTimePicker.SetValue(num2date(mintime).strftime('%X'))
        self.menu_p.str_page.endTimePicker.SetValue(num2date(maxtime).strftime('%X'))

    def OnOpenDir(self, event):
        dialog = wx.DirDialog(None, "Choose a directory:",self.dirname,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.ReactivateStreamPage()
            filelist = glob.glob(os.path.join(dialog.GetPath(),'*'))
            self.dirname = dialog.GetPath() # modify self.dirname
            files = sorted(filelist, key=os.path.getmtime)
            oldest = extractDateFromString(files[0])
            #print files[0], files[-1]
            old  = wx.DateTimeFromTimeT(time.mktime(oldest.timetuple()))
            newest = extractDateFromString(files[-1])
            new  = wx.DateTimeFromTimeT(time.mktime(newest.timetuple()))
            #print oldest, newest
            self.menu_p.str_page.pathTextCtrl.SetValue(dialog.GetPath())
            self.menu_p.str_page.fileTextCtrl.SetValue("*")
            self.menu_p.str_page.startDatePicker.SetValue(old)
            self.menu_p.str_page.startTimePicker.SetValue("00:00:00")
            self.menu_p.str_page.endDatePicker.SetValue(new)
            self.menu_p.str_page.endTimePicker.SetValue("23:59:59")
            #self.changeStatusbar("Loading data ...")
        dialog.Destroy()

    def OnOpenFile(self, event):
        self.dirname = ''
        stream = DataStream()
        stream.header = {}
        filelist = []
        dlg = wx.FileDialog(self, "Choose a file", self.dirname, "", "*.*", wx.MULTIPLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.ReactivateStreamPage()
            self.changeStatusbar("Loading data ...")
            pathlist = dlg.GetPaths()
            for path in pathlist:
                elem = os.path.split(path)
                self.dirname = elem[0]
                filelist.append(elem[1])
                tmp = read(path,tenHz=False,gpstime=True)
                stream.extend(tmp.container,tmp.header)
            #stream = read(path_or_url=os.path.join(self.dirname, self.filename),tenHz=True,gpstime=True)
            #self.menu_p.str_page.lengthStreamTextCtrl.SetValue(str(len(stream)))
            self.filename = ' ,'.join(filelist)
            self.menu_p.str_page.fileTextCtrl.SetValue(self.filename)
            self.menu_p.str_page.pathTextCtrl.SetValue(self.dirname)
            self.menu_p.str_page.fileTextCtrl.Disable()
            self.menu_p.str_page.pathTextCtrl.Disable()
            if len(stream) > 0:
                self.SetPageValues(stream)
                """
                mintime = stream._get_min('time')
                maxtime = stream._get_max('time')
                self.menu_p.str_page.startDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(num2date(mintime).timetuple())))
                self.menu_p.str_page.endDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(num2date(maxtime).timetuple())))
                self.menu_p.str_page.startTimePicker.SetValue(num2date(mintime).strftime('%X'))
                self.menu_p.str_page.endTimePicker.SetValue(num2date(maxtime).strftime('%X'))
                """
                #self.menu_p.str_page.startDatePicker.Disable()
                #self.menu_p.str_page.endDatePicker.Disable()
                #self.menu_p.str_page.startTimePicker.Disable()
                #self.menu_p.str_page.endTimePicker.Disable()
                self.menu_p.str_page.openStreamButton.Disable()

        self.menu_p.rep_page.logMsg('- %i data point loaded' % len(stream))
        dlg.Destroy()

        # plot data
        self.stream = stream
        self.plotstream = stream
        if len(stream) > 0:
            self.OnInitialPlot(stream)
            self.changeStatusbar("Ready")
        else:
            self.changeStatusbar("No data selected")


    def OnOpenURL(self, event):
        dlg = OpenWebAddressDialog(None, title='Open URL')
        if dlg.ShowModal() == wx.ID_OK:
            self.ReactivateStreamPage()
            url = dlg.urlTextCtrl.GetValue()
            if not url.endswith('/'):
                self.changeStatusbar("Loading data ...")
                self.menu_p.str_page.pathTextCtrl.SetValue(url)
                self.menu_p.str_page.fileTextCtrl.SetValue(url.split('/')[-1])
                stream = read(path_or_url=url)
                if len(stream) > 0:
                    self.SetPageValues(stream)
                    self.menu_p.str_page.openStreamButton.Disable()
                    self.OnInitialPlot(stream)
            else:
                self.menu_p.str_page.pathTextCtrl.SetValue(url)
        if len(stream) > 0:
            self.menu_p.rep_page.logMsg('- %i data point loaded' % len(stream))
            self.stream = stream
            self.plotstream = stream
        self.changeStatusbar("Ready")
        dlg.Destroy()


    def OnOpenDB(self, event):
        # a) get all DATAINFO IDs and store them in a list
        # b) disable pathTextCtrl (DB: dbname)
        # c) Open dialog which lets the user select list and time window
        # d) update stream menu
        if self.db:
            self.menu_p.rep_page.logMsg('- Accessing database ...')
            cursor = self.db.cursor()
            sql = "SELECT DataID, DataMinTime, DataMaxTime FROM DATAINFO"
            cursor.execute(sql)
            output = cursor.fetchall()
            datainfoidlist = [elem[0] for elem in output]
            if len(datainfoidlist) < 1:
                dlg = wx.MessageDialog(self, "No data tables available!\n"
                            "please check your database\n",
                            "OpenDB", wx.OK|wx.ICON_INFORMATION)
                dlg.ShowModal()
                dlg.Destroy()
                return
            dlg = DatabaseContentDialog(None, title='MySQL Database: Get content',datalst=datainfoidlist)
            if dlg.ShowModal() == wx.ID_OK:
                datainfoid = dlg.dataComboBox.GetValue()
                stream = DataStream()
                mintime = stream._testtime([elem[1] for elem in output if elem[0] == datainfoid][0])
                lastupload = stream._testtime([elem[2] for elem in output if elem[0] == datainfoid][0])
                maxtime = stream._testtime(datetime.strftime(lastupload,'%Y-%m-%d')+' 23:59:59')
                #maxtime = stream._testtime(datetime.strftime(datetime.utcnow(),'%Y-%m-%d')+' 23:59:59')
                #maxtime = datestream._testtime([elem[2] for elem in output if elem[0] == datainfoid][0])
                self.menu_p.str_page.startDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(mintime.timetuple())))
                self.menu_p.str_page.endDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(maxtime.timetuple())))
                self.menu_p.str_page.startTimePicker.SetValue(mintime.strftime('%X'))
                self.menu_p.str_page.endTimePicker.SetValue(maxtime.strftime('%X'))
                self.menu_p.str_page.pathTextCtrl.SetValue('MySQL Database')
                self.menu_p.str_page.fileTextCtrl.SetValue(datainfoid)
                self.menu_p.str_page.startDatePicker.Enable()
                self.menu_p.str_page.endDatePicker.Enable()
                self.menu_p.str_page.startTimePicker.Enable()
                self.menu_p.str_page.endTimePicker.Enable()
                self.menu_p.str_page.openStreamButton.Enable()
            dlg.Destroy()
        else:
            dlg = wx.MessageDialog(self, "Could not access database!\n"
                        "please check your connection\n",
                        "OpenDB", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()


    def OnExportData(self, event):
        dlg = ExportDataDialog(None, title='Export Data')
        if dlg.ShowModal() == wx.ID_OK:
            filenamebegins = dlg.beginTextCtrl.GetValue()
            filenameends = dlg.endTextCtrl.GetValue()
            datetyp = dlg.dateComboBox.GetValue()
            if datetyp == '2000-11-22':
                dateformat = '%Y-%m-%d'
            elif datetyp == '20001122':
                dateformat = '%Y%m%d'
            else:
                dateformat = '%b%d%y'
            path = dlg.selectedTextCtrl.GetValue()
            fileformat = dlg.formatComboBox.GetValue()
            coverage = dlg.coverageComboBox.GetValue()
            if coverage == 'hour':
                coverage = timedelta(hour=1)
            elif coverage == 'day':
                coverage = timedelta(days=1)
            elif coverage == 'year':
                coverage = timedelta(year=1)
            mode = dlg.modeComboBox.GetValue()
            #print "Stream: ", len(self.stream), len(self.plotstream)
            #print "Data: ", self.stream[0].time, self.stream[-1].time, self.plotstream[0].time, self.plotstream[-1].time
            #print "Main : ", dateformat, fileformat, coverage, mode
            try:
                self.plotstream.write(path,
                                filenamebegins=filenamebegins,
                                filenameends=filenameends,
                                dateformat=dateformat,
                                mode=mode,
                                coverage=coverage,
                                format_type=fileformat)
            except:
                self.menu_p.rep_page.logMsg("Writing failed - Permission?")
        dlg.Destroy()

    def _db_connect(self, host, user, passwd, dbname):
        try:
            self.db = MySQLdb.connect (host=host,user=user,passwd=passwd,db=dbname)
        except:
            self.db = False
        if self.db:
            self.DBOpen.Enable(True)
            self.menu_p.rep_page.logMsg('- MySQL Database selected.')
            self.changeStatusbar("Database %s successfully connected" % (dbname))
        else:
            self.menu_p.rep_page.logMsg('- MySQL Database access failed.')
            self.changeStatusbar("Database connection failed")

    def OnDBConnect(self, event):
        """
        Provide access for local network:
        Open your /etc/mysql/my.cnf file in your editor.
        scroll down to the entry:
        bind-address = 127.0.0.1
        and you can either hash that so it binds to all ip addresses assigned
        #bind-address = 127.0.0.1
        or you can specify an ipaddress to bind to. If your server is using dhcp then just hash it out.
        Then you'll need to create a user that is allowed to connect to your database of choice from the host/ip your connecting from.
        Login to your mysql console:
        milkchunk@milkchunk-desktop:~$ mysql -uroot -p
        GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY 'some_pass' WITH GRANT OPTION;
        You change out the 'user' to whatever user your wanting to use and the '%' is a hostname wildcard. Meaning that you can connect from any hostname with it. You can change it to either specify a hostname or just use the wildcard.
        Then issue the following:
        FLUSH PRIVILEGES;
        Be sure to restart your mysql (because of the config file editing):
        /etc/init.d/mysql restart
        """
        dlg = DatabaseConnectDialog(None, title='MySQL Database: Connect to')
        dlg.hostTextCtrl.SetValue(self.host)
        dlg.userTextCtrl.SetValue(self.user)
        dlg.passwdTextCtrl.SetValue(self.passwd)
        if self.db == None or self.db == 'None' or not self.db:
            dlg.dbTextCtrl.SetValue('None')
        else:
            dlg.dbTextCtrl.SetValue(self.dbname)
        if dlg.ShowModal() == wx.ID_OK:
            host = dlg.hostTextCtrl.GetValue()
            user = dlg.userTextCtrl.GetValue()
            passwd = dlg.passwdTextCtrl.GetValue()
            mydb = dlg.dbTextCtrl.GetValue()
            self._db_connect(host, user, passwd, mydb)
            """
            self.db = MySQLdb.connect (host=host,user=user,passwd=passwd,db=mydb)
            if self.db:
                self.DBOpen.Enable(True)
                self.menu_p.rep_page.logMsg('- MySQL Database selected.')
                self.changeStatusbar("Database %s successfully connected" % (self.db))
            else:
                self.menu_p.rep_page.logMsg('- MySQL Database access failed.')
                self.changeStatusbar("Database connection failed")
            """
        dlg.Destroy()

    def OnFileQuit(self, event):
        self.Close()


    def OnSave(self, event):
        textfile = open(os.path.join(self.dirname, self.filename), 'w')
        textfile.write(self.control.GetValue())
        textfile.close()

    def OnSaveAs(self, event):
        if self.askUserForFilename(defaultFile=self.filename, style=wx.SAVE,
                                   **self.defaultFileDialogOptions()):
            self.OnSave(event)


    def OnOptionsInit(self, event):
        dlg = OptionsInitDialog(None, title='Options: Parameter specifications')
        dlg.hostTextCtrl.SetValue(self.host)
        dlg.userTextCtrl.SetValue(self.user)
        dlg.passwdTextCtrl.SetValue(self.passwd)
        dlg.dbTextCtrl.SetValue(self.dbname)
        dlg.resolutionTextCtrl.SetValue(str(self.resolution))
        dlg.filenameTextCtrl.SetValue(self.filename)
        dlg.dirnameTextCtrl.SetValue(self.dirname)
        #dlg.hostTextCtrl.SetValue(self.compselect)
        #dlg.hostTextCtrl.SetValue(self.host)
        dlg.diexpDTextCtrl.SetValue(self.diexpD)
        dlg.diexpITextCtrl.SetValue(self.diexpI)
        dlg.dialphaTextCtrl.SetValue(self.dialpha)
        dlg.dideltaFTextCtrl.SetValue(self.dideltaF)
        dlg.ditypeTextCtrl.SetValue(self.ditype)
        if dlg.ShowModal() == wx.ID_OK:
            host = dlg.hostTextCtrl.GetValue()
            user = dlg.userTextCtrl.GetValue()
            passwd = dlg.passwdTextCtrl.GetValue()
            db = dlg.dbTextCtrl.GetValue()
            if db == '':
                db = 'None'
            filename=dlg.filenameTextCtrl.GetValue()
            dirname=dlg.dirnameTextCtrl.GetValue()
            resolution=dlg.resolutionTextCtrl.GetValue()
            diexpD=dlg.diexpDTextCtrl.GetValue()
            diexpI=dlg.diexpITextCtrl.GetValue()
            dialpha=dlg.dialphaTextCtrl.GetValue()
            dideltaF=dlg.dideltaFTextCtrl.GetValue()
            ditype=dlg.ditypeTextCtrl.GetValue()

            compselect= 'xyz' # compselect
            abscompselect= 'xyz' #abscompselect
            basecompselect= 'poly' #basecompselect
            saveini(host=host, user=user, passwd=passwd, dbname=db, filename=filename, dirname=dirname, compselect=compselect, abscompselect=abscompselect, basecompselect=basecompselect, diexpD=diexpD, diexpI=diexpI, dialpha=dialpha, dideltaF=dideltaF, ditype=ditype)

            inipara = loadini()
            self.initParameter(inipara)

        dlg.Destroy()


    def OnOptionsObs(self, event):
        dlg = OptionsObsDialog(None, title='Options: Observatory specifications')
        dlg.ShowModal()
        dlg.Destroy()

        #dlg = wx.MessageDialog(self, "Coming soon:\n"
        #                "Modify observatory specifications\n",
        #                "PyMag by RL", wx.OK|wx.ICON_INFORMATION)
        #dlg.ShowModal()
        #dlg.Destroy()

    def onOpenAuxButton(self, event):
        if self.askUserForFilename(style=wx.OPEN,
                                   **self.defaultFileDialogOptions()):
            #dat = read_general(os.path.join(self.dirname, self.filename), 0)
            textfile = open(os.path.join(self.dirname, self.filename), 'r')
            self.menu_p.gen_page.AuxDataTextCtrl.SetValue(textfile.read())
            textfile.close()
            #print dat

    def changeStatusbar(self,msg):
        self.SetStatusText(msg)


    # ################
    # page methods:

    # pages: stream (plot, coordinate), analysis (smooth, filter, fit, baseline etc),
    #          specials(spectrum, power), absolutes (), report (log), monitor (access web socket)


    # ################
    # Analysis functions

    def onFilterButton(self, event):
        """
        Method for filtering
        """
        self.changeStatusbar("Filtering...")
        keystr = self.menu_p.ana_page.keysTextCtrl.GetValue().encode('ascii','ignore')
        keys = keystr.split(',')

        if len(self.plotstream) == 0:
            self.plotstream = self.stream

        filtertype = self.menu_p.ana_page.selectfilterComboBox.GetValue()
        filterlength = self.menu_p.ana_page.selectlengthComboBox.GetValue()

        self.plotstream = self.plotstream.nfilter(keys=keys,filter_type=filtertype,resample=True)

        self.menu_p.rep_page.logMsg('- filtering applied')
        self.SetPageValues(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)


    def onRemoveOutlierButton(self, event):
        """
        Method for Outlier
        """
        self.changeStatusbar("Removing outliers ...")
        sr = self.menu_p.ana_page.samplingrateTextCtrl.GetValue().encode('ascii','ignore')
        keys = self.shownkeylist

        timerange = timedelta(seconds=float(sr)*120)

        if len(self.plotstream) == 0:
            self.plotstream = self.stream

        self.plotstream = self.plotstream.remove_outlier(keys=keys, timerange=timerange)
        self.plotstream = self.plotstream.remove_flagged()

        self.menu_p.rep_page.logMsg('- removed outliers: check logfile for details')
        self.SetPageValues(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)

    def onDerivativeButton(self, event):
        """
        Method for derivative
        """
        self.changeStatusbar("Calculating derivative ...")
        keys = self.shownkeylist

        if len(self.plotstream) == 0:
            self.plotstream = self.stream

        self.menu_p.rep_page.logMsg("- calculating derivative")
        self.plotstream = self.plotstream.differentiate(keys=keys,put2keys=keys)

        self.menu_p.rep_page.logMsg('- derivative calculated')
        self.SetPageValues(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)

    def onFitButton(self, event):
        """
        Method for fitting
        """
        self.changeStatusbar("Fitting ...")
        keys = self.shownkeylist
        if len(self.plotstream) == 0:
            self.plotstream = self.stream
        fitknots = str(0.5)
        fitdegree = str(4)
        fitfunc='spline'
        dlg = AnalysisFitDialog(None, title='Analysis: Fit parameter', fitfunc=fitfunc, fitknots=fitknots, fitdegree=fitdegree)
        if dlg.ShowModal() == wx.ID_OK:
            fitfunc = dlg.funcComboBox.GetValue()
            knots = dlg.knotsTextCtrl.GetValue()
            degree = dlg.degreeTextCtrl.GetValue()
            self.menu_p.rep_page.logMsg('Fitting with %s, %s, %s' % (fitfunc, knots, degree))
            if not 0<float(knots)<1:
                knots = 0.5
            if not degree>0:
                degree = 1
            else:
                degree = int(degree)
            if len(self.plotstream) > 0:
                func = self.plotstream.fit(keys=keys)
                self.SetPageValues(self.plotstream)
                self.OnPlot(self.plotstream,keys,function=func)
            else:
                # Msgbox to load data first
                pass

        dlg.Destroy()
        self.menu_p.rep_page.logMsg('- data fitted')
        self.changeStatusbar("Ready")


    def onOffsetButton(self, event):
        """
        Method for offset correction
        """
        self.changeStatusbar("Adding offsets ...")

        if len(self.plotstream) == 0:
            self.plotstream = self.stream
        keys = self.shownkeylist
        offsetdict = {}

        dlg = AnalysisOffsetDialog(None, title='Analysis: define offsets', keylst=keys)
        if dlg.ShowModal() == wx.ID_OK:
            for key in keys:
                offset = eval('dlg.'+key+'TextCtrl.GetValue()')
                if not offset == '':
                    offsetdict[key] = float(offset)

            if not len(self.plotstream) == 0 and not len(offsetdict) == 0:
                self.plotstream = self.plotstream.offset(offsetdict)
                self.SetPageValues(self.plotstream)
                self.OnPlot(self.plotstream,self.shownkeylist)

        dlg.Destroy()
        self.changeStatusbar("Ready")

    def onActivityButton(self, event):
        """
        Method for offset correction
        """
        self.changeStatusbar("Getting activity ...")

        if len(self.plotstream) == 0:
            self.plotstream = self.stream

        # open a dialog with optins and methods (fmi,cobsindex, stormtracer, etc)
        # return a method and its parameters
        #self.plotstream = self.plotstream.offset(offsetdict)

        #self.SetPageValues(self.plotstream)
        #self.OnPlot(self.plotstream,self.shownkeylist)

    def onRotationButton(self, event):
        """
        Method for offset correction
        """
        self.changeStatusbar("Rotating data ...")

        if len(self.plotstream) == 0:
            self.plotstream = self.stream

        if len(self.plotstream) > 0:
            # XXX Eventually SetValues from init
            dlg = AnalysisRotationDialog(None, title='Analysis: rotate data')
            if dlg.ShowModal() == wx.ID_OK:
                alphat = dlg.alphaTextCtrl.GetValue()
                betat = dlg.betaTextCtrl.GetValue()
                try:
                    alpha = float(alphat)
                except:
                    alpha = 0.0
                try:
                    beta = float(betat)
                except:
                    beta = 0.0

                self.plotstream = self.plotstream.rotation(alpha=alpha, beta=beta)
                self.menu_p.rep_page.logMsg('- rotated stream by alpha = %s and beta = %s' % (alphat,betat))

                self.SetPageValues(self.plotstream)
                self.OnPlot(self.plotstream,self.shownkeylist)

        dlg.Destroy()
        self.changeStatusbar("Ready")

    # ################
    # Stream functions

    def onOpenStreamButton(self, event):
        stream = DataStream()
        stday = self.menu_p.str_page.startDatePicker.GetValue()
        sttime = self.menu_p.str_page.startTimePicker.GetValue()
        sd = datetime.fromtimestamp(stday.GetTicks())
        enday = self.menu_p.str_page.endDatePicker.GetValue()
        ed = datetime.fromtimestamp(enday.GetTicks())
        path = self.menu_p.str_page.pathTextCtrl.GetValue()
        files = self.menu_p.str_page.fileTextCtrl.GetValue()

        print stday, sttime, sd

        if path == "":
            dlg = wx.MessageDialog(self, "Please select a path first!\n"
                        "go to File -> Select Dir\n",
                        "OpenStream", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            return
        if files == "":
            dlg = wx.MessageDialog(self, "Please select a file first!\n"
                        "accepted wildcards are * (e.g. *, *.dat, FGE*)\n",
                        "OpenStream", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            return

        start= datetime.strftime(sd, "%Y-%m-%d %H:%M:%S")
        end= datetime.strftime(ed, "%Y-%m-%d %H:%M:%S")


        try:
            self.changeStatusbar("Loading data ...")
            if path.endswith('/'):
                address = path
                stream = read(path_or_url=address,starttime=sd, endtime=ed)
            elif path.startswith('MySQL'):
                start= datetime.strftime(sd, "%Y-%m-%d %H:%M:%S")
                end= datetime.strftime(ed, "%Y-%m-%d %H:%M:%S")
                stream = db2stream(self.db, None, start, end, files, None)
            else:
                address = os.path.join(path,files)
                stream = read(path_or_url=address,starttime=sd, endtime=ed)
            mintime = stream._get_min('time')
            maxtime = stream._get_max('time')
            self.menu_p.str_page.startDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(num2date(mintime).timetuple())))
            self.menu_p.str_page.endDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(num2date(maxtime).timetuple())))
            self.menu_p.str_page.startTimePicker.SetValue(num2date(mintime).strftime('%X'))
            self.menu_p.str_page.endTimePicker.SetValue(num2date(maxtime).strftime('%X'))
            #self.menu_p.str_page.startDatePicker.Disable()
            #self.menu_p.str_page.endDatePicker.Disable()
            #self.menu_p.str_page.startTimePicker.Disable()
            #self.menu_p.str_page.endTimePicker.Disable()
            self.menu_p.str_page.openStreamButton.Disable()
        except:
            dlg = wx.MessageDialog(self, "Could not read file(s)!\n"
                        "check your files and/or selected time range\n",
                        "OpenStream", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            return

        self.menu_p.rep_page.logMsg('- Stream loaded of length %i' % len(stream))
        self.stream = stream
        #self.menu_p.str_page.lengthStreamTextCtrl.SetValue(str(len(stream)))
        self.OnInitialPlot(stream)


    def onReDrawButton(self, event):
        stday = self.menu_p.str_page.startDatePicker.GetValue()
        sttime = self.menu_p.str_page.startTimePicker.GetValue()
        sd = datetime.fromtimestamp(stday.GetTicks())
        enday = self.menu_p.str_page.endDatePicker.GetValue()
        entime = self.menu_p.str_page.endTimePicker.GetValue()
        ed = datetime.fromtimestamp(enday.GetTicks())

        if 'AM' or 'PM' in sttime:
            stt = datetime.strptime(sttime,'%I:%M:%S %p')
            ett = datetime.strptime(entime,'%I:%M:%S %p')
        else:
            stt = datetime.strptime(sttime,'%H:%M:%S')
            ett = datetime.strptime(entime,'%H:%M:%S')

        start = datetime.combine(datetime.date(sd), datetime.time(stt))
        end = datetime.combine(datetime.date(ed), datetime.time(ett))

        if len(self.stream) == 0:
            dlg = wx.MessageDialog(self, "Please select a path first!\n"
                        "go to File -> Select Dir\n",
                        "OpenStream", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            return

        if len(self.plotstream) == 0:
            self.plotstream = self.stream

        #st = datetime.strftime(sd, "%Y-%m-%d") + " " + sttime
        #start = datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
        #et = datetime.strftime(ed, "%Y-%m-%d") + " " + entime
        #end = datetime.strptime(et, "%Y-%m-%d %H:%M:%S")

        try:
            self.changeStatusbar("Loading data ...")
            stream = self.plotstream.trim(starttime=start, endtime=end)
            mintime = stream._get_min('time')
            maxtime = stream._get_max('time')
            self.menu_p.str_page.startDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(num2date(mintime).timetuple())))
            self.menu_p.str_page.endDatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(num2date(maxtime).timetuple())))
            self.menu_p.str_page.startTimePicker.SetValue(num2date(mintime).strftime('%X'))
            self.menu_p.str_page.endTimePicker.SetValue(num2date(maxtime).strftime('%X'))
            #self.menu_p.str_page.startDatePicker.Disable()
            #self.menu_p.str_page.endDatePicker.Disable()
            #self.menu_p.str_page.startTimePicker.Disable()
            #self.menu_p.str_page.endTimePicker.Disable()
            self.menu_p.str_page.openStreamButton.Disable()
        except:
            dlg = wx.MessageDialog(self, "Could not trim data!\n"
                        "check your files and/or selected time range\n",
                        "OpenStream", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            return

        print "Stream loaded of length ", len(stream)
        self.plotstream = stream
        self.SetPageValues(stream)
        #self.menu_p.str_page.lengthStreamTextCtrl.SetValue(str(len(stream)))
        self.OnInitialPlot(stream)


    def onSelectKeys(self,event):
        """
        open dialog to select shown keys (check boxes)
        """
        if len(self.plotstream) == 0:
            self.plotstream = self.stream
        keylist = self.plotstream._get_key_headers()
        self.keylist = keylist
        shownkeylist = self.shownkeylist
        if len(self.plotstream) > 0:
            dlg = StreamSelectKeysDialog(None, title='Select keys:',keylst=keylist,shownkeys=self.shownkeylist)
            for elem in shownkeylist:
                exec('dlg.'+elem+'CheckBox.SetValue(True)')
            if dlg.ShowModal() == wx.ID_OK:
                shownkeylist = []
                for elem in keylist:
                    boolval = eval('dlg.'+elem+'CheckBox.GetValue()')
                    if boolval:
                        shownkeylist.append(elem)
                if len(shownkeylist) == 0:
                    shownkeylist = self.shownkeylist
                else:
                    self.shownkeylist = shownkeylist
                self.SetPageValues(self.plotstream)
                self.OnPlot(self.plotstream,shownkeylist)
            dlg.Destroy()


    def onExtractData(self,event):
        """
        open dialog to choose extract parameter (paramater compare value)
        up to three possibilities
        """

        if len(self.plotstream) == 0:
            self.plotstream = self.stream
        keylist = self.shownkeylist
        if len(self.plotstream) > 0:
            dlg = StreamExtractValuesDialog(None, title='Extract:',keylst=keylist)
            if dlg.ShowModal() == wx.ID_OK:
                key1 = dlg.key1ComboBox.GetValue()
                comp1 = dlg.compare1ComboBox.GetValue()
                val1 = dlg.value1TextCtrl.GetValue()
                logic2 = dlg.logic2ComboBox.GetValue()
                logic3 = dlg.logic3ComboBox.GetValue()
                extractedstream = self.plotstream.extract(key1,val1,comp1)
                val2 = dlg.value2TextCtrl.GetValue()
                if not val2 == '':
                    key2 = dlg.key2ComboBox.GetValue()
                    comp2 = dlg.compare2ComboBox.GetValue()
                    if logic2 == 'and':
                        extractedstream = extractedstream.extract(key2,val2,comp2)
                    else:
                        extractedstream2 = self.plotstream.extract(key2,val2,comp2)
                        # TODO extractedstream = join(extractedstream,extractedstream2)
                    val3 = dlg.value3TextCtrl.GetValue()
                    if not val3 == '':
                        key3 = dlg.key3ComboBox.GetValue()
                        comp3 = dlg.compare3ComboBox.GetValue()
                        if logic3 == 'and':
                            extractedstream = extractedstream.extract(key3,val3,comp3)
                        else:
                            extractedstream3 = self.plotstream.extract(key3,val3,comp3)
                            # TODO extractedstream = join(extractedstream,extractedstream3)
                self.plotstream = extractedstream
                self.SetPageValues(self.plotstream)
                self.OnPlot(extractedstream,self.shownkeylist)
        else:
            self.menu_p.rep_page.logMsg("Extract: No data available so far")
        # specify filters -> allow to define filters Combo with key - Combo with selector (>,<,=) - TextBox with Filter

    def onChangePlotOptions(self,event):
        """
        open dialog to modify plot options (general (e.g. bgcolor) and  key
        specific (key: symbol color errorbar etc)
        """
        # specify plot options ('o','-' etc
        # show/edit meta info
        pass

    def onRestoreData(self,event):
        """
        Restore originally loaded data
        """
        if not len(self.stream) == 0:
            self.plotstream = self.stream
            self.OnInitialPlot(self.stream)

    def onChangeComp(self, event):
        orgcomp = self.compselect
        self.compselect = self.menu_p.str_page.comp[event.GetInt()]
        coordinate = orgcomp+'2'+self.compselect
        self.changeStatusbar("Transforming ...")
        if len(self.plotstream) == 0:
            self.plotstream = self.stream
        self.plotstream = self.plotstream._convertstream(coordinate)
        self.SetPageValues(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)


    # ####################
    # Absolute functions


    def onLoadDI(self,event):
        """
        open dialog to load DI data
        """
        if len(self.stream) > 0:
            pass
            # send a message box that this data will be erased

        dlg = LoadDIDialog(None, title='Get DI data')
        if dlg.ShowModal() == wx.ID_OK:
            self.menu_p.rep_page.logMsg("- loaded DI data")
            self.menu_p.abs_page.diTextCtrl.SetValue(', '.join(dlg.pathlist))
            self.dipathlist = dlg.pathlist

        if len(self.dipathlist) > 0:
            self.menu_p.abs_page.AnalyzeButton.Enable()

        dlg.Destroy()


    def onDefineVario(self,event):
        """
        open dialog to load DI data
        """
        if len(self.stream) > 0:
            pass
            # send a message box that this data will be erased

        dlg = DefineVarioDialog(None, title='Get Variometer path')
        if dlg.ShowModal() == wx.ID_OK:
            self.menu_p.abs_page.varioTextCtrl.SetValue(dlg.path)
            self.divariopath = os.path.join(dlg.path,'*')
        dlg.Destroy()


    def onDefineScalar(self,event):
        """
        open dialog to load DI data
        """
        if len(self.stream) > 0:
            pass
            # send a message box that this data will be erased
        dlg = DefineScalarDialog(None, title='Get path for scalar data')
        if dlg.ShowModal() == wx.ID_OK:
            self.menu_p.abs_page.scalarTextCtrl.SetValue(dlg.path)
            self.discalarpath = os.path.join(dlg.path,'*')
        dlg.Destroy()


    def onDIAnalyze(self,event):
        """
        open dialog to load DI data
        """
        if len(self.dipathlist) > 0:
            self.changeStatusbar("Processing DI data ...")
            #absstream = absoluteAnalysis(self.dipathlist,self.divariopath,self.discalarpath, expD=self.diexpD,expI=self.diexpI,diid=self.diid,stationid=self.stationid,abstype=self.ditype, azimuth=self.diazimuth,pier=self.dipier,alpha=self.dialpha,deltaF=self.dideltaF, dbadd=self.didbadd)
            redir=RedirectText(self.menu_p.abs_page.dilogTextCtrl)
            sys.stdout=redir
            print "Paths:", self.divariopath,self.discalarpath
            if not self.diazimuth == '':
                absstream = absoluteAnalysis(self.dipathlist,self.divariopath,self.discalarpath, expD=self.diexpD,expI=self.diexpI,stationid=self.stationid,abstype=self.ditype, azimuth=self.diazimuth,alpha=self.dialpha,deltaF=self.dideltaF)
            else:
                absstream = absoluteAnalysis(self.dipathlist,self.divariopath,self.discalarpath, expD=self.diexpD,expI=self.diexpI,stationid=self.stationid)
            # only if more than one point is selected
            self.changeStatusbar("Ready")
            if len(absstream) > 1:
                self.stream = absstream
                self.plotstream = absstream
                self.OnInitialPlot(self.stream)

            redir=RedirectText(self.menu_p.rep_page.logMsg)
            sys.stdout=redir


    def onDISetParameter(self,event):
        """
        open parameter box for DI analysis
        """

        dlg = DISetParameterDialog(None, title='Set Parameter')
        dlg.expDTextCtrl.SetValue(str(self.diexpD))
        dlg.deltaFTextCtrl.SetValue(str(self.dideltaF))
        dlg.azimuthTextCtrl.SetValue(str(self.diazimuth))
        dlg.alphaTextCtrl.SetValue(str(self.dialpha))

        if dlg.ShowModal() == wx.ID_OK:
            if not dlg.expDTextCtrl.GetValue() == '':
                self.diexpD = float(dlg.expDTextCtrl.GetValue())
            if not dlg.azimuthTextCtrl.GetValue() == '':
                self.diazimuth = float(dlg.azimuthTextCtrl.GetValue())
            if not dlg.pierTextCtrl.GetValue() == '':
                self.dipier = dlg.pierTextCtrl.GetValue()
            if not dlg.alphaTextCtrl.GetValue() == '':
                self.dialpha = float(dlg.alphaTextCtrl.GetValue())
            if not dlg.deltaFTextCtrl.GetValue() == '':
                self.dideltaF = float(dlg.deltaFTextCtrl.GetValue())

        dlg.Destroy()


'''
# To run:
app = wx.App(redirect=False)
frame = MainFrame(None,-1,"")
frame.Show()
app.MainLoop()
'''
