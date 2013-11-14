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


class AbsolutePage(wx.Panel):
    #def __init__(self, parent):
        #wx.Panel.__init__(self,parent,-1,size=(100,100))
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.comp = ['xyz', 'hdz', 'idf']
        self.createControls()
        self.doLayout()
        
    # Widgets
    def createControls(self):
        self.DrawAllAbsButton = wx.Button(self,-1,"Draw all absolutes")
        self.plotLabel = wx.StaticText(self, label="Plot options:")
        self.drawRadioBox = wx.RadioBox(self,
            label="Select vector components",
            choices=self.comp, majorDimension=3, style=wx.RA_SPECIFY_COLS)
        self.showFlaggedCheckBox = wx.CheckBox(self,label="Show flagged data")
        self.SaveFlaggedAbsButton = wx.Button(self,-1,"Save flags")
        self.dataLabel = wx.StaticText(self, label="Data options:")
        self.CalcAbsButton = wx.Button(self,-1,"Recalculate absolutes")
        self.NewAbsButton = wx.Button(self,-1,"Input absolute data")
        self.OpenAbsButton = wx.Button(self,-1,"Open absolute file")
        self.curselecteddateLabel = wx.StaticText(self, label="Current selection:")
        self.GetGraphMarksButton = wx.Button(self,-1,"Get mark")
        self.curdateTextCtrl = wx.TextCtrl(self, value="--")
        self.curcommentTextCtrl = wx.TextCtrl(self, value="-")
        self.curdateTextCtrl.Disable()
        self.curcommentTextCtrl.Disable()
        self.flagSingleButton = wx.Button(self,-1,"Flag date")

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=5, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [emptySpace,
                  emptySpace,
                 (self.DrawAllAbsButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.plotLabel, noOptions),
                  emptySpace,
                 (self.drawRadioBox, noOptions),
                  emptySpace,
                 (self.showFlaggedCheckBox, noOptions),
                  emptySpace,
                 (self.GetGraphMarksButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.flagSingleButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.curselecteddateLabel, noOptions),
                  emptySpace,
                 (self.curdateTextCtrl, expandOption),
                 (self.curcommentTextCtrl, expandOption),
                  emptySpace,
                 (self.SaveFlaggedAbsButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.dataLabel, noOptions),
                  emptySpace,
                 (self.OpenAbsButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.NewAbsButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.CalcAbsButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class BaselinePage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.varios = ['didd', 'lemi', 'gdas']
        self.func = ['bspline', 'polynom']
        self.createControls()
        self.doLayout()
        
    # Widgets
    def createControls(self):
        self.startdateLabel = wx.StaticText(self, label="Insert enddate:")
        self.startDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(datetime.strptime("2010-11-22","%Y-%m-%d").timetuple())))
        self.selectvarioLabel = wx.StaticText(self, label="Select variometer:")
        self.basevarioComboBox = wx.ComboBox(self, choices=self.varios,
            style=wx.CB_DROPDOWN, value=self.varios[0])
        self.baselineLabel = wx.StaticText(self, label="Baseline calculation:")
        self.DrawBaseButton = wx.Button(self,-1,"Calculate Basevalues")
        self.DrawBaseFuncButton = wx.Button(self,-1,"Draw Baselinefunction")
        self.funcRadioBox = wx.RadioBox(self,
            label="Select function",
            choices=self.func, majorDimension=2, style=wx.RA_SPECIFY_COLS)
        self.durationLabel = wx.StaticText(self, label="duration (basedate - x days):")
        self.durationTextCtrl = wx.TextCtrl(self, value="380")
        self.degreeLabel = wx.StaticText(self, label="Function degree:")
        self.degreeTextCtrl = wx.TextCtrl(self, value="1.2")
        self.baseweightCheckBox = wx.CheckBox(self, label="Weight fit by stddev")
        self.baseresidualCheckBox = wx.CheckBox(self, label="Plot residual")
        self.baserecalcCheckBox = wx.CheckBox(self, label="Recalculate basefunc")
        self.baselineanalysisLabel = wx.StaticText(self, label="Baseline analysis:")
        self.stabilityTestButton = wx.Button(self,-1,"Check Baselinestability")
        self.baselinestabilityLabel = wx.StaticText(self, label="requires > 30 min")

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=7, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.startdateLabel, noOptions),
                 (self.selectvarioLabel, noOptions),
                 (self.startDatePicker, noOptions),
                 (self.basevarioComboBox, expandOption),
                 (self.baselineLabel, noOptions),
                  emptySpace,
                 (self.funcRadioBox, noOptions),
                  emptySpace,
                 (self.durationLabel, noOptions),
                 (self.degreeLabel, noOptions),
                 (self.durationTextCtrl, expandOption),
                 (self.degreeTextCtrl, expandOption),
                 (self.baseweightCheckBox, noOptions),
                 (self.baseresidualCheckBox, noOptions),
                 (self.baserecalcCheckBox, noOptions),
                  emptySpace,
                 (self.DrawBaseButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.DrawBaseFuncButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.baselineanalysisLabel, noOptions),
                  emptySpace,
                 (self.stabilityTestButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.baselinestabilityLabel, noOptions),
                  emptySpace,
                  emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


 
