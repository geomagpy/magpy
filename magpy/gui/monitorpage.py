#!/usr/bin/env python

from magpy.stream import *
from magpy.core import database

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

import multiprocessing

class MonitorPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        font = wx.Font(10, wx.DEFAULT, wx.NORMAL, wx.BOLD)
        self.head1Label = wx.StaticText(self, label="Live data source:")
        self.head2Label = wx.StaticText(self, label="Run record:")
        # all buttons open dlg to add parameters (e.g. IP,
        self.getMARTASButton = wx.Button(self,-1,"Connect to MARTAS", size=(160,30)) # enabled in _db_connect
        self.getMARCOSButton = wx.Button(self,-1,"Connect to MARCOS", size=(160,30)) # disabled in _disable if paho cannot be imported
        #self.getMQTTButton = wx.Button(self,-1,"Connect to MQTT", size=(160,30))
        self.martasLabel = wx.TextCtrl(self, value="not connected", size=(160,30), style=wx.TE_RICH)  # red bg
        self.marcosLabel = wx.TextCtrl(self, value="not connected", size=(160,30), style=wx.TE_RICH)  # red bg
        #self.mqttLabel = wx.TextCtrl(self, value="not connected", size=(160,30), style=wx.TE_RICH)  # red bg
        self.marcosLabel.SetEditable(False)
        self.martasLabel.SetEditable(False)
        #self.mqttLabel.SetEditable(False)
        # Parameters if connection is established
        # 
        self.coverageLabel = wx.StaticText(self, label="Plot coverage (sec):", size=(160,30))
        self.coverageTextCtrl = wx.TextCtrl(self, value="600", size=(160,30))

        self.sliderLabel = wx.StaticText(self, label="Update period (sec):", size=(160,30))
        self.frequSlider = wx.Slider(self, -1, 10, 1, 60, (-1, -1), (100, -1),
                wx.SL_AUTOTICKS | wx.SL_HORIZONTAL | wx.SL_LABELS)

        self.startMonitorButton = wx.Button(self,-1,"Start Monitor", size=(160,30))  # if started then everything else will be disabled ..... except save monitor
        self.stopMonitorButton = wx.Button(self,-1,"Stop Monitor", size=(160,30))

        self.saveMonitorButton = wx.Button(self,-1,"Store data", size=(160,30))  # produces a bin file
        #self.startMonitorButton.Disable()
        #self.saveMonitorButton.Disable()
        # Connection Log
        # 
        self.connectionLogLabel = wx.StaticText(self, label="Connection Log:")
        self.connectionLogTextCtrl = wx.TextCtrl(self, wx.ID_ANY, size=(330,300),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.head1Label.SetFont(font)
        self.head2Label.SetFont(font)


    def doLayout(self):
        mainSizer = wx.BoxSizer(wx.VERTICAL)
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=20, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.head1Label, noOptions),
                 emptySpace,
                 (self.getMARTASButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.martasLabel, noOptions),
                 (self.getMARCOSButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.marcosLabel, noOptions),
                 #(self.getMQTTButton, dict(flag=wx.ALIGN_CENTER)),
                 #(self.mqttLabel, noOptions),
                  emptySpace,
                  emptySpace,
                 (self.coverageLabel, noOptions),
                 (self.coverageTextCtrl, expandOption),
                 (self.sliderLabel, noOptions),
                 (self.frequSlider, expandOption),
                 (self.saveMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.head2Label, noOptions),
                 emptySpace,
                 (self.startMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.stopMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                 emptySpace,
                 emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        #self.SetSizerAndFit(boxSizer)

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        mainSizer.Add(self.connectionLogLabel, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.connectionLogTextCtrl, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        self.SetSizerAndFit(mainSizer)


    def logMsg(self, message):
        ''' Private method to append a string to the logger text
            control. '''
        #print message
        self.connectionLogTextCtrl.AppendText('%s\n'%message)

