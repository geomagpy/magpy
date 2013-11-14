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
import wx.lib.masked as masked

# Subclasses for Menu pages and their controls     

class StreamPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.datatype = ['raw', 'reviewed']
        self.varios = ['didd', 'lemi', 'gdas']
        self.scalars = ['didd', 'pmag', 'csmag','none']
        self.resolution = ['intrinsic', 'second', 'minute', 'hour']
        self.comp = ['xyz', 'hdz', 'idf']
        self.createControls()
        self.doLayout()
        
    # Widgets
    def createControls(self):
        self.pathLabel = wx.StaticText(self, label="Path:")
        self.pathTextCtrl = wx.TextCtrl(self, value="")
        self.fileLabel = wx.StaticText(self, label="File:")
        self.fileTextCtrl = wx.TextCtrl(self, value="*")
        self.startdateLabel = wx.StaticText(self, label="Start date:")
        self.startDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(datetime.strptime("2011-11-22","%Y-%m-%d").timetuple())))
        self.startTimePicker = masked.timectrl.TimeCtrl(self,
                                               fmt24hr=True, id=-1, name='startTimePicker',
                                               style=0,useFixedWidthFont=True,
                                               pos = (250,70))
        self.enddateLabel = wx.StaticText(self, label="End date:")
        self.endDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(datetime.now().timetuple())))
        self.endTimePicker = masked.timectrl.TimeCtrl(self,
                                               fmt24hr=True, id=-1, name='endTimePicker',
                                               style=0,useFixedWidthFont=True,
                                               value=datetime.now().strftime('%X'), pos = (250,70))

        self.openStreamButton = wx.Button(self,-1,"Open stream")
        self.lengthStreamLabel = wx.StaticText(self, label="N (values):")
        self.lengthStreamTextCtrl = wx.TextCtrl(self, value="")

        self.DrawButton = wx.Button(self,-1,"Draw/Recalc")
        """
        self.SaveScalarButton = wx.Button(self,-1,"Save data")
        self.primaryLabel = wx.StaticText(self, label="Primary instrument:")
        self.resolutionLabel = wx.StaticText(self, label="Select resolution:")
        self.scalarComboBox = wx.ComboBox(self, choices=self.scalars,
            style=wx.CB_DROPDOWN, value=self.scalars[0])
        self.resolutionComboBox = wx.ComboBox(self, choices=self.resolution,
            style=wx.CB_DROPDOWN, value=self.resolution[0])
        self.datatypeLabel = wx.StaticText(self, label="Select datatype:")
        self.datatypeComboBox = wx.ComboBox(self, choices=self.datatype,
            style=wx.CB_DROPDOWN, value=self.datatype[1])
        self.addoptLabel = wx.StaticText(self, label="Select additional graphs:")
        self.removeOutliersCheckBox = wx.CheckBox(self,
            label="Remove Outliers")
        self.recoveryCheckBox = wx.CheckBox(self,
            label="Show data coverage")
        self.tCheckBox = wx.CheckBox(self,
            label="Plot T")
        self.showFlaggedCheckBox = wx.CheckBox(self,
            label="show flagged")
        self.secondaryLabel = wx.StaticText(self, label="Secondary instrument:")
        self.secscalarComboBox = wx.ComboBox(self, choices=self.scalars,
            style=wx.CB_DROPDOWN, value=self.scalars[3])
        self.secremoveOutliersCheckBox = wx.CheckBox(self,
            label="Remove Outliers")
        self.secremoveOutliersCheckBox.Disable()
        self.analysisLabel = wx.StaticText(self, label="Data analysis:")
        self.deltaFCheckBox = wx.CheckBox(self,
            label="show dF")
        self.deltaFCheckBox.Disable()
        self.deltaFIniLabel = wx.StaticText(self, label="Delta F (Def)")
        self.deltaFIniTextCtrl = wx.TextCtrl(self, value="2.81") # get this value from obsini
        self.deltaFIniTextCtrl.Disable()
        self.deltaFCurLabel = wx.StaticText(self, label="Delta F (Cur)")
        self.deltaFCurTextCtrl = wx.TextCtrl(self, value="NaN")
        self.deltaFCurTextCtrl.Disable()
        self.curdateTextCtrl = wx.TextCtrl(self, value="--")
        self.curdateTextCtrl.Disable()
        self.prevdateTextCtrl = wx.TextCtrl(self, value="--")
        self.prevdateTextCtrl.Disable()
        self.GetGraphMarksButton = wx.Button(self,-1,"Get marks")
        self.flagSingleButton = wx.Button(self,-1,"Flag date")
        self.flagRangeButton = wx.Button(self,-1,"Flag range")
        self.curselecteddateLabel = wx.StaticText(self, label="Current sel.")
        self.prevselecteddateLabel = wx.StaticText(self, label="Previous sel.")
        """

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=24, cols=2, vgap=5, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.pathLabel, noOptions),
                 (self.pathTextCtrl, expandOption),
                 (self.fileLabel, noOptions),
                 (self.fileTextCtrl, expandOption),
                 (self.startdateLabel, noOptions),
                  emptySpace,
                 (self.startDatePicker, expandOption),
                 (self.startTimePicker, expandOption),
                 (self.enddateLabel, noOptions),
                  emptySpace,
                 (self.endDatePicker, expandOption),
                 (self.endTimePicker, expandOption),
                 (self.openStreamButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.lengthStreamLabel, noOptions),
                 (self.lengthStreamTextCtrl, expandOption),
                 (self.DrawButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

