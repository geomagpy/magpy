#!/usr/bin/env python

from magpy.stream import *
import magpy.absolutes as di
import magpy.core.database

import wx

# wx 4.x
from wx.adv import DatePickerCtrl as wxDatePickerCtrl
from wx.adv import DP_DEFAULT as wxDP_DEFAULT

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

import wx.lib.masked as masked

# Subclasses for Menu pages and their controls

class StreamPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.comp = ['xyz', 'hdz', 'idf']
        self.symbol = ['line', 'point']
        self.flagidlist = ['0: normal data',
                        '1: automatically flagged',
                        '2: keep data in any case',
                        '3: remove data',
                        '4: special flag']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        font = wx.Font(10, wx.DEFAULT, wx.NORMAL, wx.BOLD)
        self.nextButton = wx.Button(self,-1,"next >>",size=(160,30))
        self.previousButton = wx.Button(self,-1,"<< previous",size=(160,30))
        self.infoLabel = wx.StaticText(self, label="Data information:")
        self.lineLabel1 = wx.StaticText(self, label="  ")
        self.lineLabel2 = wx.StaticText(self, label="  ")
        self.lineLabel3 = wx.StaticText(self, label="  ")
        self.lineLabel4 = wx.StaticText(self, label="  ")
        self.lineLabel5 = wx.StaticText(self, label="  ")
        self.lineLabel6 = wx.StaticText(self, label="  ")
        self.lineLabel7 = wx.StaticText(self, label="  ")
        self.lineLabel8 = wx.StaticText(self, label="  ")
        self.pathLabel = wx.StaticText(self, label="Path/Source:")
        self.pathTextCtrl = wx.TextCtrl(self, value="")
        self.fileLabel = wx.StaticText(self, label="File/Table:")
        self.fileTextCtrl = wx.TextCtrl(self, value="*")
        self.startdateLabel = wx.StaticText(self, label="Start date:")
        self.startDatePicker = wxDatePickerCtrl(self, style=wxDP_DEFAULT)
        # the following line produces error in my win xp installation
        self.startTimePicker = wx.TextCtrl(self, value="00:00:00")
        self.enddateLabel = wx.StaticText(self, label="End date:")
        self.endDatePicker = wxDatePickerCtrl(self, style=wxDP_DEFAULT)
        self.endTimePicker = wx.TextCtrl(self, value=datetime.now().strftime('%X'))
        self.trimStreamButton = wx.Button(self,-1,"Trim timerange",size=(160,30))
        self.plotOptionsLabel = wx.StaticText(self, label="Data selection:")
        #self.flagOptionsLabel = wx.StaticText(self, label="Flagging methods:")
        self.selectKeysButton = wx.Button(self,-1,"Select Columns",size=(160,30))
        self.dropKeysButton = wx.Button(self,-1,"Drop Columns",size=(160,30))
        self.extractValuesButton = wx.Button(self,-1,"Extract Values",size=(160,30))
        self.getGapsButton = wx.Button(self,-1,"Get gaps",size=(160,30))
        self.compRadioBox = wx.RadioBox(self,
            label="Coordinate system",
            choices=self.comp, majorDimension=3, style=wx.RA_SPECIFY_COLS)
        self.symbolRadioBox = wx.RadioBox(self,
            label="Select symbols",
            choices=self.symbol, majorDimension=2, style=wx.RA_SPECIFY_COLS)
        self.statsLabel = wx.StaticText(self, label="Continuous statistics:")
        self.activateStatsCheckBox = wx.CheckBox(self,label="activate",size=(160,30))
        self.compRadioBox.Disable()
        self.symbolRadioBox.Disable()

        self.infoLabel.SetFont(font)
        self.plotOptionsLabel.SetFont(font)
        self.statsLabel.SetFont(font)


    def doLayout(self):
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = '(0,0), noOptions'

        elemlist = ['self.lineLabel7, noOptions',
                    'self.lineLabel8, noOptions',
                    'self.previousButton, dict(flag=wx.ALIGN_CENTER)',
                    'self.nextButton, dict(flag=wx.ALIGN_CENTER)',
                    'self.lineLabel1, noOptions',
                    'self.lineLabel2, noOptions',
                    'self.infoLabel, noOptions',
                    '(0,0), noOptions',
                    'self.pathLabel, noOptions',
                 'self.pathTextCtrl, expandOption',
                 'self.fileLabel, noOptions',
                 'self.fileTextCtrl, expandOption',
                 'self.startdateLabel, noOptions',
                  '(0,0), noOptions',
                 'self.startDatePicker, expandOption',
                 'self.startTimePicker, expandOption',
                 'self.enddateLabel, noOptions',
                 '(0,0), noOptions',
                 'self.endDatePicker, expandOption',
                 'self.endTimePicker, expandOption',
                 'self.trimStreamButton, dict(flag=wx.ALIGN_CENTER)',
                 '(0,0), noOptions',
                 'self.lineLabel3, noOptions',
                 'self.lineLabel4, noOptions',
                 'self.plotOptionsLabel, noOptions',
                 '(0,0), noOptions',
                 'self.selectKeysButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.dropKeysButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.extractValuesButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.getGapsButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.compRadioBox, noOptions',
                 'self.symbolRadioBox, noOptions',
                 'self.lineLabel5, noOptions',
                 'self.lineLabel6, noOptions',
                    'self.statsLabel, noOptions',
                    '(0,0), noOptions',
                    'self.activateStatsCheckBox, noOptions',
                    '(0,0), noOptions']

        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        rows = int((len(elemlist)+1)/2.)
        gridSizer = wx.FlexGridSizer(rows=rows, cols=2, vgap=5, hgap=10)

        # modify look: ReDraw connected to radio and check boxes with dates
        # buttons automatically redraw the graph

        # Add the controls to the sizers:
        for elem in elemlist:
            control = elem.split(', ')[0]
            options = elem.split(', ')[1]
            gridSizer.Add(eval(control), **eval(options))

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)
