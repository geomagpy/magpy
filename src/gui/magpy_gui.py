#!/usr/bin/env python

import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')

from stream import *
from absolutes import *
from transfer import *
from database import *

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

from wx.lib.pubsub import Publisher

from gui.streampage import *
from gui.absolutespage import *
from gui.developpage import *

x = [1,2,2,1,3,6]
y = [5,6,4,3,1,5]

class ScreenSelections(object):
    def __init__(self, seldatelist=[], selvallist=[],shflag = False):
        self.seldatelist = seldatelist
        self.selvallist = selvallist
        self.shflag = shflag

    def clearList(self):
        self.seldatelist = []
        self.selvallist = []

    def updateList(self):
        print self.seldatelist[len(self.seldatelist)-1]
        #panel = wx.Panel(self,-1)
        #self.sp = MenuPanel(panel)
        #self.menu_p.rep_page.logMsg
               

class DataContainer(object):
    def __init__(self, magdatastruct1=[], magdatastruct2=[],struct1res=0,struct2res=0):
        self.magdatastruct1 = magdatastruct1
        self.magdatastruct2 = magdatastruct2
        self.struct1res = struct1res
        self.struct2res = struct2res

    def test(self):
        print len(self.magdatastruct1)
    
class FlagDateDialog(wx.Dialog):   
    #def __init__(self, parent, title, choices, curflag):
        #super(FlagDateDialog, self).__init__(parent=parent, 
        #    title=title, size=(250, 400))
    def __init__(self, parent, choices, comment, title ):
        super(FlagDateDialog, self).__init__( parent=parent, choices=[], comment='', title=title )
        self.choices = choices
        self.createControls()
        self.doLayout()
        self.bindControls()
        
    #def doLayout(self):
    #    self.logger.SetDimensions(x=10, y=20, width=200, height=300)
    # Widgets
    def createControls(self):
        # single anaylsis
        self.flagLabel = wx.StaticText(self, label="Add a flag to a single date")
        self.flagsRadioBox = wx.RadioBox(self,
            label="Select flag",
            choices=self.choices, majorDimension=2, style=wx.RA_SPECIFY_COLS)
        self.commentText =  wx.TextCtrl(self, style=wx.TE_MULTILINE|wx.TE_READONLY)
        self.okButton = wx.Button(self, label='Ok')
        self.closeButton = wx.Button(self, label='Close')

    def doLayout(self):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        gridSizer = wx.FlexGridSizer(rows=3, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.flagLabel, noOptions),
                  emptySpace,
                 (self.flagsRadioBox, noOptions),
                 (self.commentText, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.okButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        
    def OnClose(self, e):        
        self.Destroy()

class OptionsObsDialog(wx.Dialog):
    
    def __init__(self, parent, title):
        super(OptionsObsDialog, self).__init__(parent=parent, 
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()
        self.bindControls()
        
    # Widgets
    def createControls(self):
        # single anaylsis
        self.obsnameLabel = wx.StaticText(self, label="Observatory name")
        self.obsnameTextCtrl = wx.TextCtrl(self, value="--")
        self.obscodeLabel = wx.StaticText(self, label="Observatory code")
        self.obscodeTextCtrl = wx.TextCtrl(self, value="--")
        self.obscityLabel = wx.StaticText(self, label="City")
        self.obscityTextCtrl = wx.TextCtrl(self, value="--")
        self.obscountryLabel = wx.StaticText(self, label="Country")
        self.obscountryTextCtrl = wx.TextCtrl(self, value="--")
        self.obslongLabel = wx.StaticText(self, label="Longitude")
        self.obslongTextCtrl = wx.TextCtrl(self, value="--")
        self.obslatLabel = wx.StaticText(self, label="Latitude")
        self.obslatTextCtrl = wx.TextCtrl(self, value="--")
        self.obsaltLabel = wx.StaticText(self, label="Altitude")
        self.obsaltTextCtrl = wx.TextCtrl(self, value="--")
        self.obsmirenLabel = wx.StaticText(self, label="Miren")
        self.obsmirenTextCtrl = wx.TextCtrl(self, value="--")

        self.pilnamesLabel = wx.StaticText(self, label="Pillars")
        self.pilnamesTextCtrl = wx.TextCtrl(self, value="--")
        self.pillocLabel = wx.StaticText(self, label="Pillar locations")
        self.pillocTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pilazimuthLabel = wx.StaticText(self, label="Pillar azimuths")
        self.pilazimuthTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pildfLabel = wx.StaticText(self, label="delta F")
        self.pildfTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pilddLabel = wx.StaticText(self, label="delta D")
        self.pilddTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pildiLabel = wx.StaticText(self, label="delta I")
        self.pildiTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pildfepochLabel = wx.StaticText(self, label="Epoch dF")
        self.pildfepochTextCtrl = wx.TextCtrl(self, value="--")
        self.pilddirepochLabel = wx.StaticText(self, label="Epoch dV")
        self.pilddirepochTextCtrl = wx.TextCtrl(self, value="--")
        
        self.okButton = wx.Button(self, label='Ok')
        self.closeButton = wx.Button(self, label='Close')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=5, cols=8, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.obsnameLabel, noOptions),
                 (self.obscodeLabel, noOptions),
                 (self.obscityLabel, noOptions),
                 (self.obscountryLabel, noOptions),
                 (self.obslongLabel, noOptions),
                 (self.obslatLabel, noOptions),
                 (self.obsaltLabel, noOptions),
                 (self.obsmirenLabel, noOptions),
                 (self.obsnameTextCtrl, expandOption),
                 (self.obscodeTextCtrl, expandOption),
                 (self.obscityTextCtrl, expandOption),
                 (self.obscountryTextCtrl, expandOption),
                 (self.obslongTextCtrl, expandOption),
                 (self.obslatTextCtrl, expandOption),
                 (self.obsaltTextCtrl, expandOption),
                 (self.obsmirenTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.pilnamesLabel, noOptions),
                 (self.pillocLabel, noOptions),
                 (self.pilazimuthLabel, noOptions),
                 (self.pildfLabel, noOptions),
                 (self.pilddLabel, noOptions),
                 (self.pildiLabel, noOptions),
                 (self.pildfepochLabel, noOptions),
                 (self.pilddirepochLabel, noOptions),
                 (self.pilnamesTextCtrl, expandOption),
                 (self.pillocTextCtrl, expandOption),
                 (self.pilazimuthTextCtrl, expandOption),
                 (self.pildfTextCtrl, expandOption),
                 (self.pilddTextCtrl, expandOption),
                 (self.pildiTextCtrl, expandOption),
                 (self.pildfepochTextCtrl, expandOption),
                 (self.pilddirepochTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.okButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        
    def OnClose(self, e):        
        self.Destroy()


class LoadAbsDialog(wx.Dialog):
    
    def __init__(self, parent, title):
        super(LoadAbsDialog, self).__init__(parent=parent, 
            title=title, size=(250, 400))
        self.createControls()
        self.doLayout()
        self.bindControls()
        
    # Widgets
    def createControls(self):
        # single anaylsis
        self.abssingleLabel = wx.StaticText(self, label="Single anaylsis")
        self.selectAbsFile = wx.Button(self,-1,"Select absolutes measurement")
        self.overriderCheckBox = wx.CheckBox(self, label="Override header information")
        self.overriderInfo =  wx.TextCtrl(self, value="oops...")
        self.absmultiLabel = wx.StaticText(self, label="Multiple anaylsis")
        self.selectdirLabel =  wx.TextCtrl(self, value="dir")
        self.selectdirButton = wx.Button(self,-1,"Select directory")
        self.okButton = wx.Button(self, label='Ok')
        self.closeButton = wx.Button(self, label='Close')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=3, cols=3, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.abssingleLabel, noOptions),
                 (self.selectAbsFile, dict(flag=wx.ALIGN_CENTER)),
                 (self.overriderCheckBox, noOptions),
                  emptySpace,
                 (self.absmultiLabel, noOptions),
                  emptySpace,
                 (self.selectdirButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.okButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        
    def OnClose(self, e):        
        self.Destroy()

   
class PlotPanel(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        # configure graph
        self.figure = plt.figure()
        # Eventually call start graph here
        scsetmp = ScreenSelections()
        self.canvas = FigureCanvas(self,-1,self.figure)
        self.initialPlot()
        self.__do_layout()
	# Possible solutions: 1) refer to a class outside this function which generates the plot and reads the data
        # 2) generate a onDraw function with pass and a child class within the main function (e.g. OnFigDraw(PlotPanel)) which call the PlotPanel
        
    def __do_layout(self):
        # Resize graph and toolbar, create toolbar
        self.vbox = wx.BoxSizer(wx.VERTICAL)
        self.vbox.Add(self.canvas, 1, wx.LEFT | wx.TOP | wx.GROW)
        self.toolbar = NavigationToolbar2Wx(self.canvas)
        self.vbox.Add(self.toolbar, 0, wx.EXPAND)
        self.SetSizer(self.vbox)
        self.vbox.Fit(self)

        #sizer = wx.BoxSizer(wx.VERTICAL)
	#sizer.Add(self.canvas, 1, wx.EXPAND|wx.RIGHT, 0)
	#self.SetSizer(sizer)
	#self.toolbar = NavigationToolbar2Wx(self.canvas)
	#self.toolbar.Realize()
	#tw, th = self.toolbar.GetSizeTuple()
	#fw, fh = self.canvas.GetSizeTuple()
	#self.toolbar.SetSize(wx.Size(fw, th))
	#sizer.Add(self.toolbar, 0)
	#self.toolbar.update()

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
        self.axes = stream.plot(keys,figure=self.figure)
        self.canvas.draw()

    def initialPlot(self):
        """
        DEFINITION:
            loads an image for the startup screen
        """

        self.axes = self.figure.add_subplot(111)
        plt.axis("off") # turn off axis
        startupimage = 'magpy.png'
        img = imread(startupimage)
        self.axes.imshow(img)
        self.canvas.draw()

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


    def mainPlot(self,magdatastruct1,magdatastruct2,array3,xlimit,pltlist,symbol,errorbar,title):
        # add here the plt order
        # e.g.variable pltorder = [1,2,3,4,9] corresponds to x,y,z,f,t1 with len(pltorder) giving the amount
        # symbol corresponds to ['-','o'] etc defining symbols of magstruct 1 and 2
        # array3 consists of time, val1, val2: for an optional auxiliary plot of data which is not part of the magdatastructs e.g. data density
        self.axes.clear()
        self.figure.clear()
        msg = ''

        acceptedflags = [0,2,20,22]

        titleline = title
        myyfmt = ScalarFormatter(useOffset=False)
            
        t,x,y,z,f,temp1 = [],[],[],[],[],[]
        dx,dy,dz,df,flag,com = [],[],[],[],[],[]
        ctyp = "xyzf"
        try:
            nr_lines = len(magdatastruct1)
            for i in range (nr_lines):
                #if findflag(magdatastruct1[i].flag,acceptedflags):
                    t.append(magdatastruct1[i].time)
                    x.append(magdatastruct1[i].x)
                    y.append(magdatastruct1[i].y)
                    z.append(magdatastruct1[i].z)
                    f.append(magdatastruct1[i].f)
                    flag.append(magdatastruct1[i].flag)
                    com.append(magdatastruct1[i].comment)
                    temp1.append(magdatastruct1[i].t1)
                    dx.append(magdatastruct1[i].dx)
                    dy.append(magdatastruct1[i].dy)
                    dz.append(magdatastruct1[i].dz)
                    df.append(magdatastruct1[i].df)
                    ctyp = magdatastruct1[i].typ
        except:
            msg += 'Primary data file not defined'
            pass

        varlist = [t,x,y,z,f,dx,dy,dz,df,temp1]
        colorlist = ["b","g","m","c","y","k"]

        t2,x2,dx2,y2,dy2,z2,dz2,f2,df2,temp2 = [],[],[],[],[],[],[],[],[],[]
        try:
            nr_lines = len(magdatastruct2)
            ctyp2 = "xyzf"
            for i in range (nr_lines):
                #if findflag(magdatastruct2[i].flag,acceptedflags):
                    t2.append(magdatastruct2[i].time)
                    x2.append(magdatastruct2[i].x)
                    dx2.append(magdatastruct2[i].dx)
                    y2.append(magdatastruct2[i].y)
                    dy2.append(magdatastruct2[i].dy)
                    z2.append(magdatastruct2[i].z)
                    dz2.append(magdatastruct2[i].dz)
                    f2.append(magdatastruct2[i].f)
                    df2.append(magdatastruct2[i].df)
                    temp2.append(magdatastruct2[i].t1)
                    ctyp2 = magdatastruct2[i].typ
        except:
            msg += 'Secondary data file not defined'
            pass

        # get max time:
        #maxti = max([magdatastruct1[-1].time,magdatastruct2[-1].time])
        #minti = min([magdatastruct1[0].time,magdatastruct2[0].time])
        
        var2list = [t2,x2,y2,z2,f2,dx2,dy2,dz2,df2,temp2]

        nsub = len(pltlist)
        plt1 = "%d%d%d" %(nsub,1,1)

        if array3 != []:
            nsub += 1
            pltlist.append(999)

        for idx, ax in enumerate(pltlist):
            n = "%d%d%d" %(nsub,1,idx+1)
            if ax != 999:
                yplt = varlist[ax]
                yplt2 = var2list[ax]
            # check whether yplt is empty:Do something useful here (e.g. fill with 0 to length t
            ypltdat = True
            for elem in yplt:
                if is_number(elem) and np.isfinite(elem):
                    ypltdat = False
                    break
            if len(yplt) == 0 or ypltdat:
                yplt = [-999]*len(t)
            #    print " Zero length causes problems!"
            #    pass
            # Create xaxis an its label
            if idx == 0:
                self.ax = self.figure.add_subplot(n)
                if xlimit == "day":
                    self.ax.set_xlim(date2num(datetime.strptime(day + "-00-00","%Y-%m-%d-%H-%M")),date2num(datetime.strptime(day + "-23-59","%Y-%m-%d-%H-%M")))
                #else:
                #    self.ax.set_xlim(minti,maxti)
                self.a = self.ax
            else:
                self.ax = self.figure.add_subplot(n, sharex=self.a)
            if idx < len(pltlist)-1:
                setp(self.ax.get_xticklabels(), visible=False)
            else:
                self.ax.set_xlabel("Time (UTC)")
            if ax == 999:
                self.ax.plot_date(array3[:,0],array3[:,1],'g-')
                self.ax.fill_between(array3[:,0],0,array3[:,1],facecolor='green',where=np.isfinite(array3[:,1]))
                self.ax.fill_between(array3[:,0],array3[:,1],1,facecolor='red',where=np.isfinite(array3[:,1]))
            else:                
                # switch color
                self.ax.plot_date(t,yplt,colorlist[idx]+symbol[0])
                if errorbar == 1:
                    self.ax.errorbar(t,yplt,yerr=varlist[ax+4],fmt=colorlist[idx]+'o')
                self.ax.plot_date(t2,yplt2,"r"+symbol[1],markersize=4)
            # is even function for left/right
            if bool(idx & 1):
                self.ax.yaxis.tick_right()
                self.ax.yaxis.set_label_position("right")
            # choose label for y-axis
            if ax == 1 or ax == 2 or ax == 3:
                label = ctyp[idx]
            elif ax == 4 or ax == 8:
                label = "f"
            elif ax == 9:
                label = "t"
            else:
                label = "unkown"
            #if ax == 1:
            #    label = ctyp[idx]
            #except:
            #    label = "t"
            if label == "d" or label == "i":
                unit = "(deg)"
            elif label == "t":
                unit = "(deg C)"
            else:
                unit = "(nT)"
            self.ax.set_ylabel(label.capitalize()+unit)
            self.ax.get_yaxis().set_major_formatter(myyfmt)
            self.ax.af2 = self.AnnoteFinder(t,yplt,flag,self.ax)
            self.figure.canvas.mpl_connect('button_press_event', self.ax.af2)
           
        self.figure.subplots_adjust(hspace=0)

        if (max(t)-min(t) < 2):
            self.a.xaxis.set_major_formatter( matplotlib.dates.DateFormatter('%H:%M'))
        elif (max(t)-min(t) < 90):
            self.a.xaxis.set_major_formatter( matplotlib.dates.DateFormatter('%b%d'))
        else:
            self.a.xaxis.set_major_formatter( matplotlib.dates.DateFormatter('%y-%m'))
            
        
class MenuPanel(wx.Panel):
    #def __init__(self, parent):
    #    wx.Panel.__init__(self,parent,-1,size=(100,100))
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        # Create pages on MenuPanel
	nb = wx.Notebook(self,-1)
	self.gra_page = GraphPage(nb)
	self.str_page = StreamPage(nb)
	self.ana_page = AnalysisPage(nb)
	self.abs_page = AbsolutePage(nb)
	self.gen_page = GeneralPage(nb)
	self.bas_page = BaselinePage(nb)
	self.rep_page = ReportPage(nb)
	self.com_page = PortCommunicationPage(nb)
	nb.AddPage(self.str_page, "Stream")
	nb.AddPage(self.gra_page, "Variometer")
	nb.AddPage(self.ana_page, "Analysis")
	nb.AddPage(self.abs_page, "Absolutes")
	nb.AddPage(self.bas_page, "Baseline")
	nb.AddPage(self.gen_page, "Auxiliary")
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
        Publisher().subscribe(self.changeStatusbar, 'changeStatusbar')

        # The Status Bar
	self.StatusBar = self.CreateStatusBar(2, wx.ST_SIZEGRIP)
        #self.changeStatusbar("Ready")

        # Some variable initializations
        self.filename = 'noname.txt'
        self.dirname = '.'
        self.compselect = "xyz"
        self.abscompselect = "xyz"
        self.bascompselect = "bspline"

        # Initialization of data container
        self.datacont = DataContainer()
        self.scse = ScreenSelections()

        # Initialization for Com Monitor
        self.monitor_active = False
        self.com_monitor = None
        self.com_data_q = None
        self.com_error_q = None
        self.value_samples = []
        self.timer = wx.Timer(self,-1)


        # Menu Bar
        self.MainMenu = wx.MenuBar()
        self.FileMenu = wx.Menu()
        self.FileOpen = wx.MenuItem(self.FileMenu, 101, "&Open File...\tCtrl+O", "Open file", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.FileOpen)
        self.DirOpen = wx.MenuItem(self.FileMenu, 102, "Select &Directory...\tCtrl+D", "Select an existing directory", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.DirOpen)
        self.WebOpen = wx.MenuItem(self.FileMenu, 103, "Open &Web adress...\tCtrl+W", "Get data from the internet", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.WebOpen)
        self.DBOpen = wx.MenuItem(self.FileMenu, 104, "&Select Database...\tCtrl+S", "Select a MySQL database", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.DBOpen)
        self.FileMenu.AppendSeparator()
        self.FileQuitItem = wx.MenuItem(self.FileMenu, wx.ID_EXIT, "&Quit\tCtrl+Q", "Quit the program", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.FileQuitItem)
        self.MainMenu.Append(self.FileMenu, "&File")
        self.HelpMenu = wx.Menu()
        self.HelpAboutItem = wx.MenuItem(self.HelpMenu, 201, "&About...", "Display general information about the program", wx.ITEM_NORMAL)
        self.HelpMenu.AppendItem(self.HelpAboutItem)
        self.MainMenu.Append(self.HelpMenu, "&Help")
        self.OptionsMenu = wx.Menu()
        self.OptionsCalcItem = wx.MenuItem(self.OptionsMenu, 301, "&Calculation parameter", "Modify calculation parameters (e.g. filters, sensitivity)", wx.ITEM_NORMAL)
        self.OptionsMenu.AppendItem(self.OptionsCalcItem)
        self.OptionsMenu.AppendSeparator()
        self.OptionsObsItem = wx.MenuItem(self.OptionsMenu, 302, "&Observatory specifications", "Modify observatory specific initialization data (e.g. paths, pears, offsets)", wx.ITEM_NORMAL)
        self.OptionsMenu.AppendItem(self.OptionsObsItem)
        self.MainMenu.Append(self.OptionsMenu, "&Options")
        self.SetMenuBar(self.MainMenu)
        # Menu Bar end


	self.__set_properties()

        # BindingControls on the menu
        self.Bind(wx.EVT_MENU, self.OnOpenDir, self.DirOpen)
        self.Bind(wx.EVT_MENU, self.OnOpenFile, self.FileOpen)
        self.Bind(wx.EVT_MENU, self.OnOpenWeb, self.WebOpen)
        self.Bind(wx.EVT_MENU, self.OnOpenDB, self.DBOpen)
        self.Bind(wx.EVT_MENU, self.OnFileQuit, self.FileQuitItem)
        self.Bind(wx.EVT_MENU, self.OnOptionsCalc, self.OptionsCalcItem)
        self.Bind(wx.EVT_MENU, self.OnOptionsObs, self.OptionsObsItem)
        self.Bind(wx.EVT_MENU, self.OnHelpAbout, self.HelpAboutItem)
        # BindingControls on the notebooks
        #       Base Page
        self.Bind(wx.EVT_BUTTON, self.onDrawBaseButton, self.menu_p.bas_page.DrawBaseButton)
        self.Bind(wx.EVT_BUTTON, self.onDrawBaseFuncButton, self.menu_p.bas_page.DrawBaseFuncButton)
        self.Bind(wx.EVT_BUTTON, self.onStabilityTestButton, self.menu_p.bas_page.stabilityTestButton)
        self.Bind(wx.EVT_RADIOBOX, self.onBasCompchanged, self.menu_p.bas_page.funcRadioBox)
        #       Stream Page
        self.Bind(wx.EVT_BUTTON, self.onOpenStreamButton, self.menu_p.str_page.openStreamButton)
        #self.Bind(wx.EVT_BUTTON, self.onScalarDrawButton, self.menu_p.str_page.DrawButton)
        #self.Bind(wx.EVT_COMBOBOX, self.onSecscalarComboBox, self.menu_p.str_page.secscalarComboBox)
        #self.Bind(wx.EVT_BUTTON, self.onGetGraphMarksButton, self.menu_p.str_page.GetGraphMarksButton)
        #self.Bind(wx.EVT_BUTTON, self.onFlagSingleButton, self.menu_p.str_page.flagSingleButton)
        #self.Bind(wx.EVT_BUTTON, self.onFlagRangeButton, self.menu_p.str_page.flagRangeButton)
        #self.Bind(wx.EVT_BUTTON, self.onSaveScalarButton, self.menu_p.str_page.SaveScalarButton)
        #       Vario Page
        #self.Bind(wx.EVT_BUTTON, self.onGetGraphMarksButton, self.menu_p.gra_page.GetGraphMarksButton)
        #self.Bind(wx.EVT_BUTTON, self.onFlagSingleButton, self.menu_p.gra_page.flagSingleButton)
        #self.Bind(wx.EVT_BUTTON, self.onFlagRangeButton, self.menu_p.gra_page.flagRangeButton)
        self.Bind(wx.EVT_BUTTON, self.onSaveVarioButton, self.menu_p.gra_page.SaveVarioButton)
        self.Bind(wx.EVT_BUTTON, self.onGraDrawButton, self.menu_p.gra_page.DrawButton)
        self.Bind(wx.EVT_RADIOBOX, self.onGraCompchanged, self.menu_p.gra_page.drawRadioBox)
        #       Absolute PAge
        #self.Bind(wx.EVT_BUTTON, self.onFlagSingleButton, self.menu_p.abs_page.flagSingleButton)
        #self.Bind(wx.EVT_BUTTON, self.onGetGraphMarksButton, self.menu_p.abs_page.GetGraphMarksButton)
        self.Bind(wx.EVT_BUTTON, self.onSaveFlaggedAbsButton, self.menu_p.abs_page.SaveFlaggedAbsButton)
        self.Bind(wx.EVT_BUTTON, self.onDrawAllAbsButton, self.menu_p.abs_page.DrawAllAbsButton)
        self.Bind(wx.EVT_BUTTON, self.onOpenAbsButton, self.menu_p.abs_page.OpenAbsButton)
        self.Bind(wx.EVT_BUTTON, self.onNewAbsButton, self.menu_p.abs_page.NewAbsButton)
        self.Bind(wx.EVT_BUTTON, self.onCalcAbsButton, self.menu_p.abs_page.CalcAbsButton)
        self.Bind(wx.EVT_RADIOBOX, self.onAbsCompchanged, self.menu_p.abs_page.drawRadioBox)
        #        Analysis Page
        self.Bind(wx.EVT_BUTTON, self.onDrawAnalysisButton, self.menu_p.ana_page.DrawButton)
        #        Auxiliary Page
        self.Bind(wx.EVT_BUTTON, self.onOpenAuxButton, self.menu_p.gen_page.OpenAuxButton)
        
        #self.Bind(wx.EVT_CUSTOM_NAME, self.addMsg)
        # Put something on Report page
        self.menu_p.rep_page.logMsg('Begin logging...')
 
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

    # Event hanlder:
    def OnHelpAbout(self, event):
        dlg = wx.MessageDialog(self, "This program is developed for\n"
                        "geomagnetic analysis. Written by RL 2011/2012\n",
                        "About MagPy", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()

    def OnExit(self, event):
        self.Close()  # Close the main window.

    def OnSave(self, event):
        textfile = open(os.path.join(self.dirname, self.filename), 'w')
        textfile.write(self.control.GetValue())
        textfile.close()

    def OnOpenDir(self, event):
        dialog = wx.DirDialog(None, "Choose a directory:",style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.menu_p.str_page.pathTextCtrl.SetValue(dialog.GetPath())
            print dialog.GetPath()
        dialog.Destroy()
        #if self.askUserForFilename(style=wx.DirDialog,
        #                           **self.defaultFileDialogOptions()):
        #    textfile = open(os.path.join(self.dirname, self.filename), 'r')
        #    self.control.SetValue(textfile.read())
        #    textfile.close()

    def OnOpenFile(self, event):
        print "Hello"
        self.dirname = ''
        dlg = wx.FileDialog(self, "Choose a file", self.dirname, "", "*.*", wx.OPEN)
        if dlg.ShowModal() == wx.ID_OK:
            self.filename = dlg.GetFilename()
            self.dirname = dlg.GetDirectory()
            stream = read(path_or_url=os.path.join(self.dirname, self.filename))
            #f = open(os.path.join(self.dirname, self.filename), 'r')
            #self.control.SetValue(f.read())
            #f.close()
            self.menu_p.str_page.lengthStreamTextCtrl.SetValue(str(len(stream)))
            self.menu_p.str_page.fileTextCtrl.SetValue(self.filename)
            self.menu_p.str_page.pathTextCtrl.SetValue(self.dirname)
        dlg.Destroy()


    def OnOpenWeb(self, event):
        dialog = wx.DirDialog(None, "Choose a directory:",style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.menu_p.str_page.pathTextCtrl.SetValue(dialog.GetPath())
            print dialog.GetPath()
        dialog.Destroy()
        #if self.askUserForFilename(style=wx.DirDialog,
        #                           **self.defaultFileDialogOptions()):
        #    textfile = open(os.path.join(self.dirname, self.filename), 'r')
        #    self.control.SetValue(textfile.read())
        #    textfile.close()

    def OnOpenDB(self, event):
        dialog = wx.DirDialog(None, "Choose a directory:",style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.menu_p.str_page.pathTextCtrl.SetValue(dialog.GetPath())
            print dialog.GetPath()
        dialog.Destroy()
        #if self.askUserForFilename(style=wx.DirDialog,
        #                           **self.defaultFileDialogOptions()):
        #    textfile = open(os.path.join(self.dirname, self.filename), 'r')
        #    self.control.SetValue(textfile.read())
        #    textfile.close()

    def OnSaveAs(self, event):
        if self.askUserForFilename(defaultFile=self.filename, style=wx.SAVE,
                                   **self.defaultFileDialogOptions()):
            self.OnSave(event)
 

    def OnFileQuit(self, event):
	self.Close()

    def OnOptionsCalc(self, event):
        dlg = wx.MessageDialog(self, "Coming soon:\n"
                        "Modify calculation options\n",
                        "MagPy by RL", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
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
        
    def abs_test(self, event):
        xx = [10,9,8,7,6,5]
        yy = [7,3,8,3,4,2]
        FigurePlot(plot_p,xx,yy)

    def onDrawAnalysisButton(self, event):
        datastruct = []
        fstruct = []
        tmpfstruct = []
        msg = ''
        pltlist = [1,2,3]
        fval = 0 # 0: no reviewed files->only vario, 1: reviewed files

        # get from options
        duration = 380
        bspldeg = 2
        func = "bspline"
        funcweight = 1
        
        stday = self.menu_p.ana_page.startDatePicker.GetValue()
        sd = datetime.fromtimestamp(stday.GetTicks()) 
        enday = self.menu_p.ana_page.endDatePicker.GetValue()
        ed = datetime.fromtimestamp(enday.GetTicks()) 
        instr = self.menu_p.ana_page.varioComboBox.GetValue()
        finstr = self.menu_p.ana_page.scalarComboBox.GetValue()
        pass
    
    def onDrawBaseButton(self, event):
        instr = self.menu_p.bas_page.basevarioComboBox.GetValue()
        stday = self.menu_p.bas_page.startDatePicker.GetValue()
        day = datetime.strftime(datetime.fromtimestamp(stday.GetTicks()),"%Y-%m-%d")
        duration = int(self.menu_p.bas_page.durationTextCtrl.GetValue())
        degree = float(self.menu_p.bas_page.degreeTextCtrl.GetValue())
        func = "bspline"
        useweight = self.menu_p.bas_page.baseweightCheckBox.GetValue()
        #if not os.path.isfile(os.path.join(baselinepath,instr,day+"_"+str(duration)+"_"+'func.obj')):
        #    self.menu_p.rep_page.logMsg(' --- Baseline files recaluclated')
        #    GetBaseline(instr, day, duration, func, degree, useweight)
        #meandiffabs = read_magstruct(os.path.join(baselinepath,instr,"baseline_"+day+"_"+str(duration)+".txt"))
        #diffabs = read_magstruct(os.path.join(baselinepath,instr,"diff2di_"+day+"_"+str(duration)+".txt"))

        #self.plot_p.mainPlot(meandiffabs,diffabs,[],"auto",[1,2,3],['o','o'],1,"Baseline")
        #self.plot_p.canvas.draw()

    def onDrawBaseFuncButton(self, event):
        instr = self.menu_p.bas_page.basevarioComboBox.GetValue()
        stday = self.menu_p.bas_page.startDatePicker.GetValue()
        day = datetime.strftime(datetime.fromtimestamp(stday.GetTicks()),"%Y-%m-%d")
        duration = int(self.menu_p.bas_page.durationTextCtrl.GetValue())
        degree = float(self.menu_p.bas_page.degreeTextCtrl.GetValue())
        #func = self.bascompselect
        #print func
        #func = "bspline"
        useweight = self.menu_p.bas_page.baseweightCheckBox.GetValue()
        recalcselect = self.menu_p.bas_page.baserecalcCheckBox.GetValue()
        self.menu_p.rep_page.logMsg('Base func for %s for range %s minus %d days using %s with degree %s, recalc %d' % (instr,day,duration,func,degree,recalcselect))
        #if not (os.path.isfile(os.path.join(baselinepath,instr,day+"_"+str(duration)+"_"+'func.obj')) and recalcselect == False):
        #    self.menu_p.rep_page.logMsg(' --- Baseline files recaluclated')
        #    GetBaseline(instr, day, duration, func, degree, useweight)
        #meandiffabs = read_magstruct(os.path.join(baselinepath,instr,"baseline_"+day+"_"+str(duration)+".txt"))
        #modelfile = os.path.normpath(os.path.join(baselinepath,instr,day+"_"+str(duration)+"_"+'func.obj'))
        #outof = Model2Struct(modelfile,5000)

        #self.plot_p.mainPlot(meandiffabs,outof,[],"auto",[1,2,3],['o','-'],0,"Baseline function")
        #self.plot_p.canvas.draw()

    def onStabilityTestButton(self, event):
        self.menu_p.rep_page.logMsg(' --- Starting baseline stability analysis')
        stday = self.menu_p.bas_page.startDatePicker.GetValue()
        day = datetime.fromtimestamp(stday.GetTicks()) 

    def onBasCompchanged(self, event):
        self.bascompselect = self.menu_p.bas_page.func[event.GetInt()]

    def onGraCompchanged(self, event):
        self.compselect = self.menu_p.gra_page.comp[event.GetInt()]

    def onGraDrawButton(self, event):
        datastruct = []
        fstruct = []
        tmpfstruct = []
        msg = ''
        pltlist = [1,2,3]
        fval = 0 # 0: no reviewed files->only vario, 1: reviewed files

        # get from options
        duration = 380
        bspldeg = 2
        func = "bspline"
        funcweight = 1
        
        stday = self.menu_p.gra_page.startDatePicker.GetValue()
        sd = datetime.fromtimestamp(stday.GetTicks()) 
        enday = self.menu_p.gra_page.endDatePicker.GetValue()
        ed = datetime.fromtimestamp(enday.GetTicks()) 
        instr = self.menu_p.gra_page.varioComboBox.GetValue()
        finstr = self.menu_p.gra_page.scalarComboBox.GetValue()

        # 1.) Select the datafiles for the instrument
        self.menu_p.rep_page.logMsg('Starting Variometer analysis:')
        # a) produce day list
        day = sd
        daylst = []
        while ed >= day:
            daylst.append(datetime.strftime(day,"%Y-%m-%d"))
            day += timedelta(days=1)
        # b) check whether raw or mod
        datatype = self.menu_p.gra_page.datatypeComboBox.GetValue()
        loadres = self.menu_p.gra_page.resolutionComboBox.GetValue()
        #if loadres == "hour":
        #    strres = "hou"
        #elif loadres == "minute":
        #    strres = "min"
        #elif loadres == "second":
        #    strres = "sec"
        #else:
        #    strres = "raw"
        # c) if reviewed use day lst and check formats (cdf) - use raw if not available
        """
        for day in daylst:
            if datatype == 'reviewed':
                # ToDo: cdf-file read problem                
                # - check for the presence of cdf and txt files
                if os.path.exists(os.path.normpath(os.path.join(preliminarypath,instr,'va_'+day + '_'+ instr+'_' + strres + '.cdf'))):
                    loadname = os.path.normpath(os.path.join(preliminarypath,instr,'va_'+day + '_'+ instr+'_' + strres + '.cdf'))
                    struct = read_magstruct_cdf(loadname)
                    self.menu_p.rep_page.logMsg(' --- cdf for %s' % day)
                elif os.path.exists(os.path.normpath(os.path.join(preliminarypath,instr,'va_'+day + '_'+ instr+'_' + strres + '.txt'))):
                    loadname = os.path.normpath(os.path.join(preliminarypath,instr,'va_'+day + '_'+ instr+'_' + strres + '.txt'))
                    struct = read_magstruct(loadname)
                    self.menu_p.rep_page.logMsg(' --- txt for %s' % day)
                else:
                    struct = readmagdata(day+"-00:00:00",day+"-23:59:59",instr)
                    self.menu_p.rep_page.logMsg(' --- using raw data for %s' % day)
                datastruct.extend(struct)
            else:
                struct = readmagdata(day+"-00:00:00",day+"-23:59:59",instr)
                datastruct.extend(struct)
            # Get f data:
            if (finstr != 'selected vario'):
                fval = 1
                if os.path.exists(os.path.normpath(os.path.join(preliminarypath,finstr,'sc_'+day + '_'+ finstr+'_' + strres + '.cdf'))):
                    fname = os.path.normpath(os.path.join(preliminarypath,finstr,'sc_'+day + '_'+ finstr+'_' + strres + '.cdf'))
                    tmpfstruct = read_magstruct_cdf(fname)
                elif os.path.exists(os.path.normpath(os.path.join(preliminarypath,finstr,'sc_'+day + '_'+ finstr+'_' + strres + '.txt'))):
                    fname = os.path.normpath(os.path.join(preliminarypath,finstr,'sc_'+day + '_'+ finstr+'_' + strres + '.txt'))
                    tmpfstruct = read_magstruct(fname)
                fstruct.extend(tmpfstruct)
            else:
                #tmpfstruct = readmagdata(day+"-00:00:00",day+"-23:59:59",instr)
                fstruct.extend(struct)

        # 2.) Check resolution and give a warning if resolution is too low (provide choice accordingly
        res = [-999,-999]
        xa,xb = CheckTimeResolution(datastruct)
        res[0] = xb[1]
        self.datacont.struct1res = xb[1]

        primstruct = datastruct
                                
        # 3.) Filter the data
        #     a) check resolution and provide choice accordingly
        
        #     b) do the filtering
        filteropt = [1.86506,0]
        msg = ''
        filterdata = []
        
        if self.menu_p.gra_page.resolutionComboBox.GetValue() == "intrinsic":
            self.menu_p.rep_page.logMsg(' --- Using intrinsiy resolution:')
            self.menu_p.rep_page.logMsg(' --- Primary data resolution: %f sec' % (res[0]*24*3600))
            filterdata = primstruct
        else:
            if self.menu_p.gra_page.resolutionComboBox.GetValue() == "hour":
                increment = timedelta(hours=1)
                offset = timedelta(hours=0.5)
                filtertype = "linear"
                incr = ahour
            elif self.menu_p.gra_page.resolutionComboBox.GetValue() == "minute":
                increment = timedelta(minutes=1)
                offset = 0
                filtertype = "gauss"
                incr = aminute
            elif self.menu_p.gra_page.resolutionComboBox.GetValue() == "second":
                increment = timedelta(seconds=1)
                offset = 0
                filtertype = "gauss"
                incr = asecond
            # Prim data
            if (res[0] < incr*0.9):
                filterdata, msg = filtermag(increment,offset,datastruct,filtertype,[],filteropt)
                self.menu_p.rep_page.logMsg(' --- Filtering primary data\n %s' % msg)
            else:
                self.menu_p.rep_page.logMsg(' --- Primary data resolution equal or larger then requested: Skipping filtering')
                filterdata = primstruct

        primstruct = filterdata
        # Filtered data to short for hour data  - didd !! 

        # 4.) Baselinecorrection
        corrdata = []
        bc = self.menu_p.gra_page.baselinecorrCheckBox.GetValue()
        if bc == True:
            # a) get the approporate baseline file  - if not exisiting create it
            endyear = datetime.strftime(ed,"%Y")
            if (datetime.strftime(sd,"%Y")) == (datetime.strftime(ed,"%Y")):
                if endyear == datetime.strftime(datetime.utcnow(),"%Y"):
                    # case -- 1a: sd to ed range within current year
                    day = datetime.strftime(datetime.utcnow(),"%Y-%m-%d")
                else:
                    # case -- 1b: sd to ed range within one year
                    day = datetime.strftime(datetime.strptime(str(int(endyear)+1),"%Y"),"%Y-%m-%d")
            else:
                td = ed-sd
                # case -- 2a: sd to ed range not within one year
                if int(td.days) > duration:
                    # case -- 2b: sd to ed range not within one year and differ by more then 380 days
                    self.menu_p.rep_page.logMsg(' --- Standard duration of baseline exceeded - using %s days now' % td.days)
                    duration = td.days
                day = datetime.strftime(ed,"%Y-%m-%d")
            if not os.path.isfile(os.path.normpath(os.path.join(baselinepath,instr,day+"_"+str(duration)+"_"+'func.obj'))):
                self.menu_p.rep_page.logMsg(' --- Creating baseline file')
                GetBaseline(instr, day, duration, func, bspldeg, funcweight)
            self.menu_p.rep_page.logMsg(' --- used Baseline: %s' % (day+"_"+str(duration)+"_"+'func.obj'))
            # use a day list with selected day for last input parameter
            dayl = sd
            daylst = []
            while ed >= dayl:
                daylst.append(datetime.strftime(dayl,"%Y-%m-%d"))
                dayl += timedelta(days=1)
            for dayl in daylst:
                cdata = BaselineCorr(instr,os.path.join(baselinepath,instr,day+"_"+str(duration)+"_"+'func.obj'),dayl)
                corrdata.extend(cdata)
        else:
            corrdata = primstruct

        primstruct = corrdata

        # 5.) F (if T is available)
        if fstruct == []:
            self.menu_p.rep_page.logMsg(' --- Use Scalar analysis first')
            dlg = wx.MessageDialog(self, "For using F you need to conduct the scalar analysis first:\n produce -reviewed- scalar data",
                        "PyMag by RL", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            self.menu_p.gra_page.fCheckBox.Disable()
            self.menu_p.gra_page.dfCheckBox.Disable()
            newdatastruct = primstruct
            mf = float("nan")
        elif fval == 0:
            # only variovalues available
            self.menu_p.rep_page.logMsg(' --- Can only use vario x,y,z')
            self.menu_p.gra_page.fCheckBox.Enable()
            self.menu_p.gra_page.dfCheckBox.Disable()
            newdatastruct,mf,fmsg = combineVarioandF(primstruct,fstruct,fval,[0,2,20,22])
        else:
            newdatastruct,mf,fmsg = combineVarioandF(primstruct,fstruct,fval,[0,2,20,22])
            self.menu_p.gra_page.fCheckBox.Enable()
            self.menu_p.gra_page.dfCheckBox.Enable()
            self.menu_p.rep_page.logMsg(' --- Combination of Vario and F data:\n %s' % fmsg)

        
        valobsini = 10
        if self.menu_p.gra_page.baselinecorrCheckBox.GetValue():
            var = 'DI'
        else:
            var = self.menu_p.gra_page.varioComboBox.GetValue()
        self.menu_p.gra_page.dfIniTextCtrl.SetValue('dF(%s - %s): %.2f nT' % (var,finstr,valobsini))
        self.menu_p.gra_page.dfCurTextCtrl.SetValue('dF(cur): %.2f nT' % mf)
        primstruct = newdatastruct

        #for i in range(10,1000):
        #    print primstruct[i].f,primstruct[i].flag
            
        drawf = self.menu_p.gra_page.fCheckBox.GetValue()
        if drawf == True:
            pltlist.append(4)
        drawdf = self.menu_p.gra_page.dfCheckBox.GetValue()
        if drawdf == True:
            pltlist.append(8)

        # 6.) Draw temperature function (if T is available)
        # check whether temp data is available
        drawt = self.menu_p.gra_page.tCheckBox.GetValue()
        if drawt == True:
            pltlist.append(9)

        # 7.) Showing flagged data
        secdata = []
        seconddata = []
        flagging = self.menu_p.gra_page.showFlaggedCheckBox.GetValue()
        if flagging:
            try:
                acceptedflags = [0,1,2,3,10,11,12,13,20,21,22,23,30,31,32,33]
                secdata, msg = filterFlag(primstruct,acceptedflags)
                self.menu_p.rep_page.logMsg(' --- flagged data added \n %s' % msg)
            except:
                self.menu_p.rep_page.logMsg(' --- Unflagging failed')
                pass
     
        # 8.) Changing coordinatesystem
        self.menu_p.rep_page.logMsg('Vario: Selected %s' % self.compselect)
        if (self.compselect == "xyz"):
            showdata = primstruct
            if secdata != []:
                seconddata = secdata
        elif (self.compselect == "hdz"):
            showdata = convertdatastruct(primstruct,"xyz2hdz")
            if secdata != []:
                seconddata = convertdatastruct(secdata,"xyz2hdz")
        elif (self.compselect == "idf"):
            showdata = convertdatastruct(primstruct,"xyz2idf")
            if secdata != []:
                seconddata = convertdatastruct(secdata,"xyz2idf")
        else:
            showdata = primstruct
            if secdata != []:
                seconddata = secdata

        self.datacont.magdatastruct1 = primstruct

        displaydata, filtmsg = filterFlag(showdata,[0,2,10,12,20,22,30,32])

        self.plot_p.mainPlot(displaydata,seconddata,[],"auto",pltlist,['-','-'],0,"Variogram")
        self.plot_p.canvas.draw()
        """

    def onSaveVarioButton(self, event):
        self.menu_p.rep_page.logMsg('Save button pressed')
        if len(self.datacont.magdatastruct1) > 0:
            # 1.) open format choice dialog
            choicelst = [ 'txt', 'cdf',  'netcdf' ]
            # iaga and wdc do not make sense for scalar values
            # ------ Create the dialog
            dlg = wx.SingleChoiceDialog( None, message='Save data as', caption='Choose dataformat', choices=choicelst)
            # ------ Show the dialog
            if dlg.ShowModal() == wx.ID_OK:
                response = dlg.GetStringSelection()

                # 2.) message box informing about predefined path
                # a) generate predefined path and name: (scalar, instr, mod, resolution
                firstday = datetime.strptime(datetime.strftime(num2date(self.datacont.magdatastruct1[0].time).replace(tzinfo=None),"%Y-%m-%d"),"%Y-%m-%d")
                lastday = datetime.strptime(datetime.strftime(num2date(self.datacont.magdatastruct1[-1].time).replace(tzinfo=None),"%Y-%m-%d"),"%Y-%m-%d")
                tmp,res = CheckTimeResolution(self.datacont.magdatastruct1)
                loadres = self.menu_p.gra_page.resolutionComboBox.GetValue()
                if loadres == 'intrinsic':
                    resstr = 'raw'
                else:
                    resstr = GetResolutionString(res[1])
                instr = self.menu_p.str_page.scalarComboBox.GetValue()
                # b) create day list
                day = firstday
                daylst = []
                while lastday >= day:
                    daylst.append(datetime.strftime(day,"%Y-%m-%d"))
                    day += timedelta(days=1)

                # 3.) save data
                if not os.path.exists(os.path.normpath(os.path.join(preliminarypath,instr))):
                    os.makedirs(os.path.normpath(os.path.join(preliminarypath,instr)))

                curnum = 0
                for day in daylst:
                    savestruct = []
                    idx = curnum
                    for idx, elem in enumerate(self.datacont.magdatastruct1):
                        if datetime.strftime(num2date(elem.time), "%Y-%m-%d") == day:
                            savestruct.append(self.datacont.magdatastruct1[idx])
                            curnum = idx

                    if response == "txt":
                        savename = os.path.normpath(os.path.join(preliminarypath,instr,'va_'+day + '_'+ instr+'_' + resstr + '.txt'))
                        write_magstruct(savename,savestruct)
                        self.menu_p.rep_page.logMsg('Saved %s data for %s' % (response,day))
                    if response == "cdf":
                        savename = os.path.normpath(os.path.join(preliminarypath,instr,'va_'+day + '_'+ instr+'_' + resstr + '.cdf'))
                        write_magstruct_cdf(savename,savestruct)
                        self.menu_p.rep_page.logMsg('Saved %s data for %s' % (response,day))
       
                # 4.) Open a message Box to inform about save

            # ------ Destroy the dialog
            dlg.Destroy()


    # ################
    # Stream functions

    def onOpenStreamButton(self, event):
        stday = self.menu_p.str_page.startDatePicker.GetValue()
        sd = datetime.fromtimestamp(stday.GetTicks()) 
        enday = self.menu_p.str_page.endDatePicker.GetValue()
        ed = datetime.fromtimestamp(enday.GetTicks()) 
        path = self.menu_p.str_page.pathTextCtrl.GetValue()
        files = self.menu_p.str_page.fileTextCtrl.GetValue()
        
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

        print os.path.join(path,files)
        stream = read(path_or_url=os.path.join(path,files))
        self.menu_p.str_page.lengthStreamTextCtrl.SetValue(str(len(stream)))

        #self.plot_p.mainPlot(display1data,display2data,ardat,"auto",pltlist,['-','-'],0,"Magnetogram")
        #self.plot_p.canvas.draw()

        #fig = stream.plot(['t1'],noshow=True)
        self.plot_p.guiPlot(stream,["t1"])
        #self.plot_p.canvas.draw()

        try:
            stream = read(path_or_url=os.path.join(path,files),starttime=stday, endtime=enday)
        except:
            dlg = wx.MessageDialog(self, "Could not read file(s)!\n"
                        "check your files and/or selected time range\n",
                        "OpenStream", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            return

    """
    def onScalarDrawButton(self, event):
        stday = self.menu_p.str_page.startDatePicker.GetValue()
        sd = datetime.fromtimestamp(stday.GetTicks()) 
        enday = self.menu_p.str_page.endDatePicker.GetValue()
        ed = datetime.fromtimestamp(enday.GetTicks()) 
        
        instr = self.menu_p.str_page.scalarComboBox.GetValue()
        secinstr = self.menu_p.str_page.secscalarComboBox.GetValue()
        data1 = []
        data2 = []
        pltlist = [4]
        datastruct = []
        secdatastruct = []
        display1data = []
        display2data = []
        msg =''
        self.scse.seldatlist=[] # clear the list : does not work yet
        self.menu_p.str_page.curdateTextCtrl.SetValue('--')
        self.menu_p.str_page.prevdateTextCtrl.SetValue('--')
        sf = self.menu_p.str_page.showFlaggedCheckBox.GetValue()

        # ToDo: check this
        self.scse.shflag = sf

        # 1.) Select the datafiles for the instrument
        self.menu_p.rep_page.logMsg('Starting Scalar analysis:')
        # a) produce day list
        day = sd
        daylst = []
        while ed >= day:
            daylst.append(datetime.strftime(day,"%Y-%m-%d"))
            day += timedelta(days=1)
        # b) check whether raw or mod
        datatype = self.menu_p.str_page.datatypeComboBox.GetValue()
        loadres = self.menu_p.str_page.resolutionComboBox.GetValue()
        if loadres == "hour":
            strres = "hou"
        elif loadres == "minute":
            strres = "min"
        elif loadres == "second":
            strres = "sec"
        else:
            strres = "raw"
        # c) if reviewed use day lst and check formats (cdf) - use raw if not available
        for day in daylst:
            if datatype == 'reviewed':
                # ToDo: cdf-file read problem                
                # - check for the presence of cdf and txt files
                if os.path.exists(os.path.normpath(os.path.join(preliminarypath,instr,'sc_'+day + '_'+ instr+'_' + strres + '.cdf'))):
                    loadname = os.path.normpath(os.path.join(preliminarypath,instr,'sc_'+day + '_'+ instr+'_' + strres + '.cdf'))
                    struct = read_magstruct_cdf(loadname)
                    self.menu_p.rep_page.logMsg(' --- cdf for %s' % day)
                elif os.path.exists(os.path.normpath(os.path.join(preliminarypath,instr,'sc_'+day + '_'+ instr+'_' + strres + '.txt'))):
                    loadname = os.path.normpath(os.path.join(preliminarypath,instr,'sc_'+day + '_'+ instr+'_' + strres + '.txt'))
                    struct = read_magstruct(loadname)
                    self.menu_p.rep_page.logMsg(' --- txt for %s' % day)
                else:
                    struct = readmagdata(day+"-00:00:00",day+"-23:59:59",instr)
                    self.menu_p.rep_page.logMsg(' --- using raw data for %s' % day)
                datastruct.extend(struct)

                if secinstr != 'none':
                    self.menu_p.rep_page.logMsg(' - Loading secondary data')
                    if os.path.exists(os.path.normpath(os.path.join(preliminarypath,secinstr,'sc_'+day + '_'+ secinstr+'_' + strres + '.cdf'))):
                        loadname = os.path.normpath(os.path.join(preliminarypath,secinstr,'sc_'+day + '_'+ secinstr+'_' + strres + '.cdf'))
                        secstruct = read_magstruct_cdf(loadname)
                        self.menu_p.rep_page.logMsg(' --- cdf for %s' % day)
                    elif os.path.exists(os.path.normpath(os.path.join(preliminarypath,secinstr,'sc_'+day + '_'+ secinstr+'_' + strres + '.txt'))):
                        loadname = os.path.normpath(os.path.join(preliminarypath,secinstr,'sc_'+day + '_'+ secinstr+'_' + strres + '.txt'))
                        secstruct = read_magstruct(loadname)
                        self.menu_p.rep_page.logMsg(' --- txt for %s' % day)
                    else:
                        secstruct = readmagdata(day+"-00:00:00",day+"-23:59:59",secinstr)
                        self.menu_p.rep_page.logMsg(' --- using raw data for %s' % day)
                    secdatastruct.extend(secstruct)
            else:
                struct = readmagdata(day+"-00:00:00",day+"-23:59:59",instr)
                datastruct.extend(struct)
                if secinstr != 'none':
                    secstruct = readmagdata(day+"-00:00:00",day+"-23:59:59",secinstr)
                    secdatastruct.extend(secstruct)
                                
        # 2.) Check resolution and give a warning if resolution is too low (provide choice accordingly
        res = [-999,-999]
        xa,xb = CheckTimeResolution(datastruct)
        res[0] = xb[1]
        self.datacont.struct1res = xb[1]

        if secinstr != 'none' and len(secdatastruct) > 2:
            ya,yb = CheckTimeResolution(secdatastruct)
            res[1] = yb[1]
            self.datacont.struct2res = yb[1]
        lowestres = max(res)

        primstruct = datastruct
        secstruct = secdatastruct

        # 3.) Remove outliers if selected
        removeoutliers = self.menu_p.str_page.removeOutliersCheckBox.GetValue()
        secremoveoutliers = self.menu_p.str_page.secremoveOutliersCheckBox.GetValue()
        if removeoutliers == True:
            datastep1, olmsg = RemoveOutliers(datastruct,1,4)
            self.menu_p.rep_page.logMsg(' -- Following Outliers removed for %s' % instr)
            self.menu_p.rep_page.logMsg(' --- \n%s ---' % olmsg)
        else:
            datastep1 = datastruct 
        if secremoveoutliers == True:
            secdatastep1, ol2msg = RemoveOutliers(secdatastruct,1,4)
            self.menu_p.rep_page.logMsg(' -- Following Outliers removed for %s' % secinstr)
            self.menu_p.rep_page.logMsg(' --- \n%s ---' % ol2msg)
        else:
            secdatastep1 = secdatastruct 

        primstruct = datastep1
        secstruct = secdatastep1

        # 4.) Filter the data
        #     a) check resolution and provide choice accordingly
        
        #     b) do the filtering
        filteropt = [1.86506,0]
        msg = ''
        secmsg = ''
        filterdata = []
        secfilterdata = []
        
        if self.menu_p.str_page.resolutionComboBox.GetValue() == "intrinsic":
            self.menu_p.rep_page.logMsg(' --- Using intrinsiy resolution:')
            self.menu_p.rep_page.logMsg(' --- Primary data resolution: %f sec' % (res[0]*24*3600))
            filterdata = primstruct
            if secinstr != 'none':
                self.menu_p.rep_page.logMsg(' --- Secondary data resolution: %f sec' % (res[1]*24*3600))
                secfilterdata = secstruct
        else:
            if self.menu_p.str_page.resolutionComboBox.GetValue() == "hour":
                increment = timedelta(hours=1)
                offset = timedelta(hours=0.5)
                incr = ahour
            elif self.menu_p.str_page.resolutionComboBox.GetValue() == "minute":
                increment = timedelta(minutes=1)
                offset = 0
                incr = aminute
            elif self.menu_p.str_page.resolutionComboBox.GetValue() == "second":
                increment = timedelta(seconds=1)
                offset = 0
                incr = asecond
            # Prim data
            if (res[0] < incr*0.9):
                filterdata, msg = filtermag(increment,offset,datastep1,"linear",[],filteropt)
                self.menu_p.rep_page.logMsg(' --- Filtering primary data\n %s' % msg)
            else:
                self.menu_p.rep_page.logMsg(' --- Primary data resolution equal or larger then requested: Skipping filtering')
                filterdata = primstruct
            # Sec data        
            if secinstr != 'none' and secdatastep1 != []:
                if (res[1] < incr*0.9):
                    secfilterdata, secmsg = filtermag(increment,offset,secdatastep1,"linear",[],filteropt)
                    self.menu_p.rep_page.logMsg(' --- Filtering secondary data\n %s' % secmsg)
                else:
                    self.menu_p.rep_page.logMsg(' --- Secondary data resolution equal or larger then requested: Skipping filtering')
                    secfilterdata = secstruct

        primstruct = filterdata
        secstruct = secfilterdata

        # 5.) Calculate dF if secondary file is activated
        submsg = ''
        if secinstr != 'none':
            showdata,meandf,submsg = subtractStructs(primstruct,secstruct,[0,1,2,3,20,21,22,23])
            self.menu_p.str_page.deltaFCurTextCtrl.SetValue(str(meandf))
            calcdF = self.menu_p.str_page.deltaFCheckBox.GetValue()
            if calcdF == True and secinstr != 'none':           
                pltlist.append(8)
        else:
            showdata = primstruct

        primstruct = showdata

        # 6.) Draw recovery function (calculation should be earlier)
        # add the recovvalue in var2 100% down to 0% relative to main spacing
        # no - different way:
        # use an independent array as recovery is not part of the magdatastruct
        recovery = self.menu_p.str_page.recoveryCheckBox.GetValue()
        if recovery:
            recovwidth = 20 # width defines a factor (width/2*resolution) which specifies the window for counting availbale data
            aa,bb = densityTimeFunc(primstruct,recovwidth)
            xyz = [aa,bb]
            ardat = array(zip(*xyz))
        else:
            ardat = array([])
        

        # 7.) Draw temperature function (if T is available)
        drawt = self.menu_p.str_page.tCheckBox.GetValue()
        if drawt == True:
            pltlist.append(9)

       # 7.) Add data to container
        self.datacont.magdatastruct1 = primstruct
        self.datacont.magdatastruct2 = secstruct

        display2data, filtmsg = filterFlag(secstruct,[0,2,20,22])
        print len(display2data)

        # 8.) Showing flagged data
        secdata = []
        flagging = self.menu_p.str_page.showFlaggedCheckBox.GetValue()
        if flagging:
            try:
                acceptedflags = [0,1,2,3,10,11,12,13,20,21,22,23,30,31,32,33]
                secdata, msg = filterFlag(primstruct,acceptedflags)
                self.menu_p.rep_page.logMsg(' --- flagged data added \n %s' % msg)
                #self.menu_p.str_page.secremoveOutliersCheckBox.SetValue('False')
                #self.menu_p.str_page.secremoveOutliersCheckBox.Disable()
                #self.menu_p.str_page.secscalarComboBox.Disable()
                display2data = secdata
            except:
                self.menu_p.rep_page.logMsg(' --- Unflagging failed')
                pass
        else:
            self.menu_p.str_page.secscalarComboBox.Enable()

        display1data, filtmsg = filterFlag(primstruct,[0,2,20,21,22,23])
                          
        self.plot_p.mainPlot(display1data,display2data,ardat,"auto",pltlist,['-','-'],0,"Magnetogram")
        self.plot_p.canvas.draw()

    def onSecscalarComboBox(self, event):
        if event.GetString() != 'none':
            self.menu_p.str_page.deltaFCheckBox.Enable()
            self.menu_p.str_page.secremoveOutliersCheckBox.Enable()
        else:
            self.menu_p.str_page.deltaFCheckBox.SetValue(False)
            self.menu_p.str_page.deltaFCheckBox.Disable()
            self.menu_p.str_page.secremoveOutliersCheckBox.SetValue(False)
            self.menu_p.str_page.secremoveOutliersCheckBox.Disable()
        self.menu_p.rep_page.logMsg('Secondary inst: %s' % event.GetString())

            
    def onGetGraphMarksButton(self, event):
        # 1.) Get the Parent page (useful for using the same function from different pages
        btn = event.GetEventObject()
        print '\n----  OnButton_GetMarks() for', btn.GetLabelText()
        self.page = btn.GetParent()
        self.page.curdateTextCtrl.SetValue('Two beers please')
        # 2.) Get dates
        self.scse = ScreenSelections()
        self.sellength = len(self.scse.seldatelist)

        if self.sellength >= 2:
            ti2 = self.scse.seldatelist[self.sellength-2]
            dat2 = datetime.strftime(num2date(ti2).replace(tzinfo=None),"%Y-%m-%d_%H:%M:%S")
            try: # in case of Absolute page this wont work
                self.page.prevdateTextCtrl.SetValue(dat2)
            except:
                pass
        if self.sellength >= 1:
            ti1 = self.scse.seldatelist[self.sellength-1]
            dat1 = datetime.strftime(num2date(ti1).replace(tzinfo=None),"%Y-%m-%d_%H:%M:%S")
            self.page.curdateTextCtrl.SetValue(dat1)
            try:
                com = 'not found'
                #load the comment from file
                for elem in self.datacont.magdatastruct1:
                    if ti1 == elem.time:
                        curflag = elem.flag
                        curcomm = elem.comment
                        com = curflag +": "+curcomm
                        break
                self.page.curcommentTextCtrl.SetValue(com)
            except:
                pass

    def onFlagSingleButton(self, event):
        # 1.) Get the Parent page (useful for using the same function from different pages
        btn = event.GetEventObject()
        self.menu_p.rep_page.logMsg('\n----  OnButton_FlagSingle() for %s' % btn.GetLabelText())
        self.page = btn.GetParent()

        date = self.page.curdateTextCtrl.GetValue()
        try:
            numdate = date2num(datetime.strptime(date,"%Y-%m-%d_%H:%M:%S"))

            for idx, elem in enumerate(self.datacont.magdatastruct1):
                if numdate == elem.time:
                    number = idx
                    curflag = elem.flag
                    break

            # in case of raw data a flag 0 is used
            #    -> changed to range 10 to 19 for scalar
            #    -> changed to range 20 to 29 for vario
            
            pagestr = str(self.page)
            if pagestr.find("Scalar") > 5:
                choicelst = [ '0: unchanged', '10: automatically removed',  '20: force use', '30: force remove' ]
            else:
                choicelst = [ '0: unchanged', '1: automatically removed',  '2: force use', '3: force remove' ]

            # Create the dialog
            dlg = wx.SingleChoiceDialog( None, message='Current flag: %s' % str(curflag), caption='Choose flag for %s' % date, choices=choicelst)
            # Show the dialog
            num = ''
            if dlg.ShowModal() == wx.ID_OK:
                response = dlg.GetStringSelection()
                num = response.split(":")[0]
                self.datacont.magdatastruct1[number].flag = int(num)
            # Destroy the dialog
            dlg.Destroy()

            if num != '':
                dlg = wx.TextEntryDialog(self, 'comment:', 'Edit comments', 
                         style=wx.TE_MULTILINE|wx.OK|wx.CANCEL)
                dlg.SetValue(self.datacont.magdatastruct1[number].comment)
                if dlg.ShowModal() == wx.ID_OK:
                    newcomment = dlg.GetValue()
                    self.datacont.magdatastruct1[number].comment = newcomment
                self.menu_p.rep_page.logMsg(' Changed flag on %s to %s: %s' % (date,response,newcomment))      
        except:
            pass
        
    def onFlagRangeButton(self, event):
        # 1.) Get the Parent page (useful for using the same function from different pages
        btn = event.GetEventObject()
        self.menu_p.rep_page.logMsg('\n----  OnButton_FlagRange() for %s' % btn.GetLabelText())
        self.page = btn.GetParent()

        date = self.page.curdateTextCtrl.GetValue()
        prevdate = self.page.prevdateTextCtrl.GetValue()

        if date == '--':
            self.menu_p.rep_page.logMsg(' --- Flagging not possible: Choose dates first!')
            return
        
        try:
            numdate = date2num(datetime.strptime(date,"%Y-%m-%d_%H:%M:%S"))
            prevnumdate = date2num(datetime.strptime(prevdate,"%Y-%m-%d_%H:%M:%S"))
            # Sort the date
            datelst = sort([numdate,prevnumdate])
            
            for idx, elem in enumerate(self.datacont.magdatastruct1):
                if datelst[0] == elem.time:
                    stnumber = idx
                    curflag = elem.flag
                if datelst[1] == elem.time:
                    ednumber = idx
                    break

            # in case of raw data a flag 0 is used
            #    -> changed to range 10 to 90 for scalar
            #    -> changed to range 0 to 9 for vario
            # 00: good, 10: Scalar removed, Vario OK; 11: Scalar and Vario removed; etc
            # Absolutesfile uses 0 to 9

            pagestr = str(self.page)
            if pagestr.find("Scalar") > 5:
                choicelst = [ '0: use data', '10: automatically removed',  '20: force use', '30: force remove', '90: keep all flags but add comments' ]
            else:
                choicelst = [ '0: use data', '1: automatically removed',  '2: force use', '3: force remove', '9: keep all flags but add comments']

            # Create the dialog
            dlg = wx.SingleChoiceDialog( None, message='Current flag: %s' % str(curflag), caption='Choose flag for %s' % date, choices=choicelst)
            # Show the dialog
            num = ''
            if dlg.ShowModal() == wx.ID_OK:
                response = dlg.GetStringSelection()
                num = response.split(":")[0]
                for i in range(stnumber,ednumber):
                    if int(num) != 90 or  int(num) != 9:
                        self.datacont.magdatastruct1[i].flag = int(num)
            # Destroy the dialog
            dlg.Destroy()

            if num != '':
                dlg = wx.TextEntryDialog(self, 'comment:', 'Edit comments', 
                         style=wx.TE_MULTILINE|wx.OK|wx.CANCEL)
                dlg.SetValue(self.datacont.magdatastruct1[stnumber].comment)
                if dlg.ShowModal() == wx.ID_OK:
                    newcomment = dlg.GetValue()
                    for i in range(stnumber,ednumber):
                        self.datacont.magdatastruct1[i].comment = newcomment
                self.menu_p.rep_page.logMsg(' Changed flag on %s to %s: %s' % (date,response,newcomment))      
        except:
            pass


    def onSaveScalarButton(self, event):
        self.menu_p.rep_page.logMsg('Save button pressed')
        if len(self.datacont.magdatastruct1) > 0:
            # 1.) open format choice dialog
            choicelst = [ 'txt', 'cdf',  'netcdf' ]
            # iaga and wdc do not make sense for scalar values
            # ------ Create the dialog
            dlg = wx.SingleChoiceDialog( None, message='Save data as', caption='Choose dataformat', choices=choicelst)
            # ------ Show the dialog
            if dlg.ShowModal() == wx.ID_OK:
                response = dlg.GetStringSelection()
            # ------ Destroy the dialog
            dlg.Destroy()

            # 2.) message box informing about predefined path
            # a) generate predefined path and name: (scalar, instr, mod, resolution
            firstday = datetime.strptime(datetime.strftime(num2date(self.datacont.magdatastruct1[0].time).replace(tzinfo=None),"%Y-%m-%d"),"%Y-%m-%d")
            lastday = datetime.strptime(datetime.strftime(num2date(self.datacont.magdatastruct1[-1].time).replace(tzinfo=None),"%Y-%m-%d"),"%Y-%m-%d")
            tmp,res = CheckTimeResolution(self.datacont.magdatastruct1)
            loadres = self.menu_p.str_page.resolutionComboBox.GetValue()
            if loadres == 'intrinsic':
                resstr = 'raw'
            else:
                resstr = GetResolutionString(res[1])
            instr = self.menu_p.str_page.scalarComboBox.GetValue()
            # b) create day list
            day = firstday
            daylst = []
            while lastday >= day:
                daylst.append(datetime.strftime(day,"%Y-%m-%d"))
                day += timedelta(days=1)

            # 3.) save data
            if not os.path.exists(os.path.normpath(os.path.join(preliminarypath,instr))):
                os.makedirs(os.path.normpath(os.path.join(preliminarypath,instr)))

            curnum = 0
            for day in daylst:
                savestruct = []
                idx = curnum
                for idx, elem in enumerate(self.datacont.magdatastruct1):
                    if datetime.strftime(num2date(elem.time), "%Y-%m-%d") == day:
                        savestruct.append(self.datacont.magdatastruct1[idx])
                        curnum = idx
                    
                if response == "txt":
                    savename = os.path.normpath(os.path.join(preliminarypath,instr,'sc_'+day + '_'+ instr+'_' + resstr + '.txt'))
                    write_magstruct(savename,savestruct)
                    self.menu_p.rep_page.logMsg('Saved %s data for %s' % (response,day))
                if response == "cdf":
                    savename = os.path.normpath(os.path.join(preliminarypath,instr,'sc_'+day + '_'+ instr+'_' + resstr + '.cdf'))
                    write_magstruct_cdf(savename,savestruct)
                    self.menu_p.rep_page.logMsg('Saved %s data for %s' % (response,day))
                    
            # 4. Open a message Box to inform about save        
    """

    # ####################
    # Absolute functions
    
    def onAbsCompchanged(self, event):
        self.abscompselect = self.menu_p.abs_page.comp[event.GetInt()]

    def onDrawAllAbsButton(self, event):
        # 1.) Load data
        """
        meanabs = read_magstruct(os.path.normpath(os.path.join(abssummarypath,'absolutes.out')))
        self.menu_p.rep_page.logMsg('Absolute Anaylsis: Selected %s' % self.abscompselect)
         2.) Select components
        if (self.abscompselect == "xyz"):
            showdata = meanabs
        elif (self.abscompselect == "hdz"):
            showdata = convertdatastruct(meanabs,"xyz2hdz")
        elif (self.abscompselect == "idf"):
            showdata = convertdatastruct(meanabs,"xyz2idf")
        else:
            showdata = meanabs
        # 3.) Select flagging
        secdata = []
        flagging = self.menu_p.abs_page.showFlaggedCheckBox.GetValue()
        if flagging:
            try:
                acceptedflags = [1,3]
                secdata, msg = filterFlag(showdata,acceptedflags)
                print len(secdata)
                self.menu_p.rep_page.logMsg(' --- flagged data added \n %s' % msg)
            except:
                self.menu_p.rep_page.logMsg(' --- Unflagging failed')
                pass

        # 4.) Add data to container
        # use xyz data here
        #self.datacont.magdatastruct1 = meanabs

        #display1data, filtmsg = filterFlag(showdata,[0,2])

        #self.plot_p.mainPlot(display1data,secdata,[],"auto",[1,2,3],['o','o'],0,"Absolutes")
        """
        self.plot_p.canvas.draw()

    def onSaveFlaggedAbsButton(self, event):
        self.menu_p.rep_page.logMsg(' --- Saving data - soon')
        savestruct=[]
        #for elem in self.datacont.magdatastruct1:
        #    savestruct.append(elem)
        #savename = os.path.normpath(os.path.join(abssummarypath,'absolutes.out'))
        #write_magstruct(savename,savestruct)
        self.menu_p.rep_page.logMsg('Saved Absolute file')

    def onCalcAbsButton(self, event):
        chgdep = LoadAbsDialog(None, title='Load Absolutes')
        chgdep.ShowModal()
        chgdep.Destroy()        

        #abslist = read_general(os.path.join(abssummarypath,'absolutes.out'),0)
        #meanabs = extract_absolutes(abslist)
        #self.plot_p.mainPlot(meanabs,[],[],"auto",[1,2,3],['o','o'],0,"Absolutes")
        self.plot_p.canvas.draw()

    def onNewAbsButton(self, event):
        abslist = read_general(os.path.join(abssummarypath,'absolutes.out'),0)
        meanabs = extract_absolutes(abslist)
        self.plot_p.mainPlot(meanabs,[],[],"auto",[1,2,3],['o','o'],0,"Absolutes")
        self.plot_p.canvas.draw()

    def onOpenAbsButton(self, event):
        abslist = read_general(os.path.join(abssummarypath,'absolutes.out'),0)
        meanabs = extract_absolutes(abslist)
        self.plot_p.mainPlot(meanabs,[],[],"auto",[1,2,3],['o','o'],0,"Absolutes")
        self.plot_p.canvas.draw()

    def addMsg(self, msg):
        print 'Got here'
        #mf = MainFrame(None,"PyMag")
        #mf.changeStatusbar('test')
        #self.menu_p.str_page.deltaFTextCtrl.SetValue('test')
        #self.menu_p.rep_page.logMsg('msg')
        #self.menu_p.str_page.deltaFTextCtrl.SendTextUpdatedEvent()

        
app = wx.App(redirect=False)
frame = MainFrame(None,-1,"")
frame.Show()
app.MainLoop()
