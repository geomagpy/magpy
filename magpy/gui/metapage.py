#!/usr/bin/env python

from magpy.stream import *
from magpy.absolutes import *
from magpy.transfer import *
from magpy.database import *

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

import wx.lib.masked as masked

# Subclasses for Menu pages and their controls

class MetaPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.comp = ['xyz', 'hdz', 'idf']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.head1Label = wx.StaticText(self, label="Basic Information:")
        self.head2Label = wx.StaticText(self, label="Database:")
        self.head3Label = wx.StaticText(self, label="Modify/Review:")
        # 1. Section
        self.samplingrateLabel = wx.StaticText(self, label="Samp. period (sec):")
        self.samplingrateTextCtrl = wx.TextCtrl(self, value="--",size=(160,30))
        self.amountLabel = wx.StaticText(self, label="N of data point:")
        self.amountTextCtrl = wx.TextCtrl(self, value="--",size=(160,30))
        self.typeLabel = wx.StaticText(self, label="Datatype:")
        self.typeTextCtrl = wx.TextCtrl(self, value="--",size=(160,30))
        self.keysLabel = wx.StaticText(self, label="Used keys:")
        self.keysTextCtrl = wx.TextCtrl(self, value="--",size=(160,30))
        self.samplingrateTextCtrl.Disable()
        self.amountTextCtrl.Disable()
        self.typeTextCtrl.Disable()
        self.keysTextCtrl.Disable()

        self.getDBButton = wx.Button(self,-1,"Get from DB",size=(160,30))
        self.putDBButton = wx.Button(self,-1,"Write to DB",size=(160,30))

        self.MetaDataButton = wx.Button(self,-1,"Data related",size=(160,30))
        self.dataTextCtrl = wx.TextCtrl(self, wx.ID_ANY, size=(330,80),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.MetaSensorButton = wx.Button(self,-1,"Sensor related",size=(160,30))
        self.sensorTextCtrl = wx.TextCtrl(self, wx.ID_ANY, size=(330,80),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.MetaStationButton = wx.Button(self,-1,"Station related",size=(160,30))
        self.stationTextCtrl = wx.TextCtrl(self, wx.ID_ANY, size=(330,80),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)


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
                 (self.samplingrateLabel, noOptions),
                 (self.samplingrateTextCtrl, expandOption),
                 (self.amountLabel, noOptions),
                 (self.amountTextCtrl, expandOption),
                 (self.typeLabel, noOptions),
                 (self.typeTextCtrl, expandOption),
                 (self.keysLabel, noOptions),
                 (self.keysTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                 # section 2
                 (self.head2Label, noOptions),
                  emptySpace,
                 (self.getDBButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.putDBButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.head3Label, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        #self.SetSizerAndFit(boxSizer)

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        mainSizer.Add(self.MetaDataButton, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.dataTextCtrl, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.MetaSensorButton, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.sensorTextCtrl, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.MetaStationButton, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.stationTextCtrl, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        self.SetSizerAndFit(mainSizer)

